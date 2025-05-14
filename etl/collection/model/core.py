"""Model for collections."""

import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Union

import fastjsonschema
import pandas as pd
import yaml
from owid.catalog.meta import GrapherConfig
from structlog import get_logger
from typing_extensions import Self

from apps.chart_sync.admin_api import AdminAPI
from etl.collection.exceptions import DuplicateCollectionViews
from etl.collection.model.base import MDIMBase, pruned_json
from etl.collection.model.dimension import Dimension, DimensionChoice
from etl.collection.model.view import CommonView, View, ViewIndicators
from etl.collection.utils import (
    get_complete_dimensions_filter,
    map_indicator_path_to_id,
    unique_records,
    validate_indicators_in_db,
)
from etl.config import OWID_ENV, OWIDEnv
from etl.files import yaml_dump
from etl.paths import EXPORT_DIR, SCHEMAS_DIR

# Logging
log = get_logger()


@pruned_json
@dataclass
class Definitions(MDIMBase):
    common_views: Optional[List[CommonView]] = None

    def __post_init__(self):
        # Validate that there is no duplicate common view (based on dimensions)
        if self.common_views is not None:
            if not (
                isinstance(self.common_views, list) and all(isinstance(view, CommonView) for view in self.common_views)
            ):
                raise TypeError("`common_views` must be a list!")

            records = []
            for view in self.common_views:
                records.append(view.dimensions)

            df = pd.DataFrame.from_records([r if r else {} for r in records])

            if df.duplicated().any():
                info = df[df.duplicated()].to_dict(orient="records")
                raise ValueError(f"Duplicate common views found! {info}")


@pruned_json
@dataclass
class Collection(MDIMBase):
    """Overall MDIM/Explorer config"""

    dimensions: List[Dimension]
    views: List[View]
    catalog_path: str
    title: Dict[str, str]
    default_selection: List[str]

    _definitions: Definitions

    topic_tags: Optional[List[str]] = None

    # Internal use. For save() method.
    _collection_type: Optional[str] = field(init=False, default="multidim")

    _group_operations_done: int = field(init=False, default=0)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Self:
        """Coerce the dictionary into the expected shape before passing it to the parent class."""
        # Make a shallow copy so we don't mutate the user's dictionary in-place
        data = dict(d)

        # If dictionary contains field 'definitions', change it for '_definitions'
        if "definitions" in data:
            data["_definitions"] = data["definitions"]
            del data["definitions"]
        else:
            data["_definitions"] = Definitions()

        # Now that data is in the expected shape, let the parent class handle the rest
        return super().from_dict(data)

    def __post_init__(self):
        # Sanity check
        assert "#" in self.catalog_path, "Catalog path should be in the format `path#name`."

    @property
    def definitions(self) -> Definitions:
        return self._definitions

    @property
    def v(self):
        return self.views

    @property
    def d(self):
        return self.dimensions

    @property
    def local_config_path(self) -> Path:
        # energy/latest/energy_prices#energy_prices -> export/multidim/energy/latest/energy_prices/config.yml
        if self._collection_type is None:
            raise ValueError("_collection_type must have a value!")
        collection_dir = "explorers" if self._collection_type == "explorer" else self._collection_type
        return EXPORT_DIR / collection_dir / (self.catalog_path.replace("#", "/") + ".config.json")

    @property
    def short_name(self):
        _, name = self.catalog_path.split("#")
        return name

    @property
    def schema_path(self) -> Path:
        return SCHEMAS_DIR / f"{self._collection_type}-schema.json"

    def save_config_local(self) -> None:
        log.info(f"Exporting config to {self.local_config_path}")
        self.save_file(self.local_config_path)

    def save(  # type: ignore[override]
        self,
        owid_env: Optional[OWIDEnv] = None,
        tolerate_extra_indicators: bool = False,
        prune_choices: bool = True,
        prune_dimensions: bool = True,
    ):
        # Ensure we have an environment set
        if owid_env is None:
            owid_env = OWID_ENV

        # Prune non-used dimension choices
        if prune_choices:
            self.prune_dimension_choices()

        # TODO: Prune dimensions if only one choice is in use
        if prune_dimensions:
            self.prune_dimensions()

        # Ensure that all views are in choices
        self.validate_views_with_dimensions()

        # Validate duplicate views
        self.check_duplicate_views()

        # Sort views based on dimension order
        self.sort_views_based_on_dimensions()

        # Check that no choice name or slug is repeated
        self.validate_choice_uniqueness()

        # Check that all indicators in explorer exist
        indicators = self.indicators_in_use(tolerate_extra_indicators)
        validate_indicators_in_db(indicators, owid_env.engine)

        # Export config to local directory in addition to uploading it to MySQL for debugging.
        self.save_config_local()

        # Upsert to DB
        self.upsert_to_db(owid_env)

    def upsert_to_db(self, owid_env: OWIDEnv):
        # Replace especial fields URIs with IDs (e.g. sortColumnSlug).
        # TODO: I think we could move this to the Grapher side.
        config = replace_catalog_paths_with_ids(self.to_dict())

        # Convert config from snake_case to camelCase
        config = camelize(config, exclude_keys={"dimensions"})

        # Upsert config via Admin API
        admin_api = AdminAPI(owid_env)
        admin_api.put_mdim_config(self.catalog_path, config)

    def to_dict(self, encode_json: bool = False, drop_definitions: bool = True) -> Dict[str, Any]:  # type: ignore
        dix = super().to_dict(encode_json=encode_json)
        if drop_definitions:
            dix = {k: v for k, v in dix.items() if k not in {"_definitions", "definitions"}}
        return dix

    def get_dimension(self, slug: str) -> Dimension:
        """Get dimension `slug`"""
        for dim in self.dimensions:
            if dim.slug == slug:
                return dim
        raise ValueError(f"Dimension {slug} not found in dimensions!")

    def get_choice_names(self, dimension_slug: str) -> Dict[str, str]:
        """Get all choice names in the collection."""
        dimension = self.get_dimension(dimension_slug)
        choice_names = {}
        for choice in dimension.choices:
            choice_names[choice.slug] = choice.name
        return choice_names

    def validate_views_with_dimensions(self):
        """Validates that dimensions in all views are valid:

        - TODO: The dimension slugs in all views are defined.
        - The dimension choices in all views are defined.
        """
        # Get all dimension and choice slugs
        dix = {dim.slug: dim.choice_slugs for dim in self.dimensions}

        # Iterate over all views and validate dimensions and choices
        for view in self.views:
            for dim_slug, choice_slugs in dix.items():
                # Check that dimension is defined in the view!
                assert (
                    dim_slug in view.dimensions
                ), f"Dimension {dim_slug} not found in dimensions! View:\n{yaml_dump(view.to_dict())}"
                # Check that choices defined in the view are valid!
                assert (
                    view.dimensions[dim_slug] in choice_slugs
                ), f"Choice {view.dimensions[dim_slug]} not found for dimension {dim_slug}! View: {view.to_dict()}; Available choices: {choice_slugs}"

    def validate_schema(self, schema_path: Optional[Union[str, Path]] = None):
        """Validate class against schema."""
        if schema_path is None:
            schema_path = self.schema_path
        with open(schema_path) as f:
            s = f.read()

            # Add "file://" prefix to "dataset-schema.json#"
            # This is needed to activate file handler below. Unfortunately, fastjsonschema does not
            # support file references out of the box
            s = s.replace("dataset-schema.json#", "file://dataset-schema.json#")

            schema = json.loads(s)

        # file handler for file:// URIs
        def file_handler(uri):
            # Remove 'file://' prefix and build local path relative to the schema file
            local_file = SCHEMAS_DIR / Path(uri.replace("file://", "")).name
            with local_file.open() as f:
                return json.load(f)

        # Pass custom format for date validation
        # NOTE: we use fastjsonschema because schema uses multiple $ref to an external schema.
        #   python-jsonschema doesn't cache external resources and is extremely slow. It should be
        #   possible to speed it up by pre-loading schema and inserting it dynamically if
        #   fastjsonschema becomes hard to maintain.
        validator = fastjsonschema.compile(
            schema, handlers={"file": file_handler}, formats={"date": r"^\d{4}-\d{2}-\d{2}$"}
        )

        try:
            validator(self.to_dict())  # type: ignore
        except fastjsonschema.JsonSchemaException as e:
            raise ValueError(f"Config validation error: {e.message}")  # type: ignore

    def indicators_in_use(self, tolerate_extra_indicators: bool = False):
        # Get all indicators used in all views
        indicators = []
        for view in self.views:
            indicators.extend(view.indicators_used(tolerate_extra_indicators))

        # Make sure indicators are unique
        indicators = list(set(indicators))

        return indicators

    def check_duplicate_views(self):
        """Check for duplicate views in the collection."""
        check_duplicate_views(self.views)

    def sort_choices(self, slug_order: Dict[str, Union[List[str], Callable]]):
        """Sort choices based on the given order."""
        not_expected = set(slug_order).difference(self.dimension_slugs)
        if not_expected:
            raise ValueError(
                f"Dimension slug{'s' if len(not_expected) > 1 else ''} {not_expected} not found in dimensions! Available dimensions are: {self.dimension_slugs}"
            )

        for dim in self.dimensions:
            if dim.slug in slug_order:
                dim.sort_choices(slug_order[dim.slug])

    def sort_views_based_on_dimensions(self):
        priority_order = self.dimension_choices

        def sort_key(view):
            # For each dimension, get the index in the priority list
            return tuple(
                priority_order[dim].index(view.dimensions.get(dim, ""))
                if view.dimensions.get(dim, "") in priority_order[dim]
                else float("inf")
                for dim in priority_order
            )

        self.views = sorted(self.views, key=sort_key)

    def validate_choice_uniqueness(self):
        """Validate that all choice names (and slugs) are unique."""
        for dim in self.dimensions:
            dim.validate_unique_names()
            dim.validate_unique_slugs()

    def validate_choice_names(self):
        """Validate that all choice names are unique."""
        for dim in self.dimensions:
            dim.validate_unique_names()

    def prune_dimensions(self):
        """Remove dimension if only one of its choice is in use."""
        # Get all dimension choices in use
        all_occurrences = self.dimension_choices_in_use()

        # Remove those not in use
        for dim in self.dimensions:
            if len(all_occurrences[dim.slug]) == 1:
                # Remove from dimensions
                self.dimensions.remove(dim)
                # Remove from views
                for view in self.views:
                    if dim.slug in view.dimensions:
                        del view.dimensions[dim.slug]

    def prune_dimension_choices(self):
        """Remove all dimension choices that are not used in any view."""
        all_occurrences = self.dimension_choices_in_use()

        # Remove those not in use
        for dim in self.dimensions:
            dim.choices = [choice for choice in dim.choices if choice.slug in all_occurrences[dim.slug]]

    @property
    def dimension_slugs(self):
        return [dim.slug for dim in self.dimensions]

    @property
    def dimension_choices(self) -> Dict[str, List[str]]:
        """Get all dimension choices in the collection."""
        return {dim.slug: [choice.slug for choice in dim.choices] for dim in self.dimensions}

    def dimension_choices_in_use(self) -> Dict[str, Set[str]]:
        from collections import defaultdict

        # Get all dimension choices in use
        all_occurrences = defaultdict(set)

        for view in self.views:
            for key, value in view.dimensions.items():
                all_occurrences[str(key)].add(value)

        return dict(all_occurrences)

    def drop_views(
        self,
        dimensions: Union[Dict[str, Union[List[str], str]], List[Dict[str, Union[List[str], str]]]],
    ):
        """Remove views that have any set of dimensions that can be generated from the given in `dimensions`.

        Args:
            dimensions (Dict[str, Union[List[str], str]]): Dictionary with the dimensions to drop. The keys are the dimension slugs, and the values are either a list of choices or a single choice.
                    - Example 1: `dimensions = {"sex": "female"}`.
                        Drop all views that have "female" in dimension sex.
                    - Example 2: `dimensions = {"age": ["0-4", "5-9"]}`.
                        Drop all views that have "0-4" OR "5-9" in dimension age.
                    - Example 3: `dimensions = {"age": ["0-4", "5-9"], "sex": ["female", "male"]}`.
                        Drop all views that have ("0-4" OR "5-9" in dimension age) AND ("female" OR "male" in dimension "sex"). The rest is kept.
                    - Example 4: `dimensions = [{"sex": "female", "age": "0-4"}, {"sex": "male", "age": "5-9"}]`.
                        Drop all views that have { ("0-4" in dimension "age") AND ("female" in dimension "sex") } OR { ("5-9" in dimension "age") AND ("male" in dimension "sex") }
        """
        # Get dimensions in use, and list of dimension slugs (ordered, used for key-identifying views)
        dimensions_available = self.dimension_choices_in_use()

        # Make sure we are dealing with a list
        if isinstance(dimensions, dict):
            dimensions = [dimensions]

        # Get list of dimension arrangements to drop: Iterate over each dimension filter, and obtain explicit filter.
        dimensions_drop = []
        for dimensions_ in dimensions:
            dimensions_drop_ = get_complete_dimensions_filter(dimensions_available, dimensions_)
            dimensions_drop.extend(dimensions_drop_)
        dimensions_drop = unique_records(dimensions_drop)

        # Function to get key for each view
        dimensions_order = list(dimensions_available.keys())

        def _get_view_key(dimension_choices: Dict[str, str]):
            return tuple(dimension_choices[dim] for dim in dimensions_order)

        # Convert the list to set of IDs. Each element in the set identifies a dimension arrangement by a tuple with the choices.
        drop_keys = {_get_view_key(dimension_drop) for dimension_drop in dimensions_drop}

        # Iterate over all views and drop those that match the given dimensions
        new_views = []
        for view in self.views:
            key = _get_view_key(view.dimensions)
            if key not in drop_keys:
                new_views.append(view)
        self.views = new_views

    def group_views(self, params: List[Dict[str, Any]], drop_dimensions_if_single_choice: bool = True):
        """Group views into new ones.

        Group views in a single view to show an aggregate view combining multiple choices for a given dimension. It takes all the views where the `dimension` choice is one of `choices` and groups them together to create a new one.

        Args:
            params (List[Dict[str, Any]]): List of dictionaries with the following keys:
                    - dimension: str
                        Slug of the dimension that contains the choices to group.
                    - choices: List[str]
                        Slugs of the choices to group.
                    - choice_new_slug: str
                        The slug for the newly created choice. If the MDIM config file doesn't specify a name, it will be the same as the slug.
                    - config_new: Optional[Dict[str, Any]], default=None
                        The view config for the new choice. E.g. useful to tweak the chart type.
                    - replace: Optional[bool], default=False
                        If True, the original choices will be removed and replaced with the new choice. If False, the original choices will be kept and the new choice will be added.
                    - overwrite_dimension_choice: Optional[bool], default=False
                        If True and `choice_new_slug` already exists as a `choice` in `dimension`, views created here will overwrite those already existing if there is any collision.

        Example:
        --------

        Suppose you have two dimensions 'sex' and 'age' with the following choices:

            sex: 'female', 'male'
            age: '0-4', '5-9', '10-14'

        Each view shows a single timeseries. E.g. for sex="female" and age="0-4", you observe a timeseries for the indicator for these specific dimension choices.

        Now, you now want to create a new view, with sex="combined", where the user can observe both timeseries (female and male) in a single view. Hence you'd end up with the following choices:

            sex: 'female', 'male', 'combined'
            age: '0-4', '5-9', '10-14'


        In this example, we have `dimension="sex"`, `choices=["female", "male"]`, and `choice_new_slug="combined"`.


        """
        new_views_all = []
        for p in params:
            assert "dimension" in p, "Dimension must be provided!"
            assert "choices" in p, "Dimension must be provided!"
            assert "choice_new_slug" in p, "Dimension must be provided!"
            dimension = p["dimension"]
            choices = p["choices"]
            choice_new_slug = p["choice_new_slug"]
            config_new = p.get("config_new")

            # Sanity checks
            self._sanity_check_view_grouping(
                dimension=dimension,
                choices=choices,
                choice_new_slug=choice_new_slug,
            )

            # Create new views
            new_views_ = self.create_new_grouped_views(
                dimension=dimension,
                choices=choices,
                choice_new_slug=choice_new_slug,
                config_new=config_new,
            )
            new_views_all.append(
                {
                    "overwrite": p.get("overwrite_dimension_choice", False),
                    "views": new_views_,
                    "dimension": dimension,
                    "choice_new": choice_new_slug,
                }
            )

            # Add dimensions. TODO: Use combine_dimensions instead?
            for dim in self.dimensions:
                if (dim.slug == dimension) and (choice_new_slug not in dim.choice_slugs):
                    new_choice = DimensionChoice(
                        slug=choice_new_slug,
                        name=choice_new_slug,
                    )
                    # Add new choice to dimension
                    dim.choices.append(new_choice)
                    break

        # Get list of new views from dictionary. Also, drop views in existing MDIM if needed (e.g. if there are collisions and overwrite_dimension_choice=True)
        new_views_list = []
        for new_views in new_views_all:
            if new_views["overwrite"]:
                # 1) TODO: Get list of dimensions to drop
                dimensions_drop = [v.dimensions for v in new_views["views"]]
                # 2) Drop dimensions
                self.drop_views(dimensions_drop)
            else:
                try:
                    check_duplicate_views(new_views["views"] + self.views)
                except DuplicateCollectionViews:
                    raise DuplicateCollectionViews(
                        f"Duplicate views found (dimension `{new_views['dimension']}`, new choice `{new_views['choice_new']}`)! If you want to overwrite the existing views, set `overwrite_dimension_choice=True` in the parameters."
                    )
            # Add views to list
            new_views_list.extend(new_views["views"])

        # Extend views
        self.views.extend(new_views_list)

        # Remove original choices if asked to
        for p in params:
            if p.get("replace", False):
                dimension = p["dimension"]
                choices = p["choices"]
                # Remove views with old choices
                new_views = [view for view in self.views if view.dimensions[dimension] not in choices]
                # Remove unused choices
                self.prune_dimension_choices()

        # Drop dimension if it has only one choice in use
        if drop_dimensions_if_single_choice:
            self.prune_dimensions()

    def _sanity_check_view_grouping(
        self,
        dimension: str,
        choices: List[str],
        choice_new_slug: str,
    ):
        # Sanity checks
        dimension_choices = self.dimension_choices
        # Check that the dimension exists
        if dimension not in set(dimension_choices):
            raise ValueError(f"Dimension {dimension} not found in dimensions!")

        # Check that the choices exist
        if not all([choice in dimension_choices[dimension] for choice in choices]):
            raise ValueError(
                f"Choices {choices} not found in dimension {dimension}! Available choices are: {dimension_choices[dimension]}"
            )

    def create_new_grouped_views(
        self,
        dimension: str,
        choices: List[str],
        choice_new_slug: str,
        config_new: Optional[GrapherConfig] = None,
    ) -> List[View]:
        """Create new grouped views."""
        if self._group_operations_done > 0:
            log.warning(
                "If you are doing more than one group operation, consider using `group_views` instead. It is optimized for batch operations, where each grouping is done in parallel."
            )
        # Prepare groups of views
        grouped = defaultdict(list)
        for view in self.views:
            # Build a key excluding the dimension we're grouping
            key = tuple((k, v) for k, v in view.dimensions.items() if k != dimension)
            if view.dimensions[dimension] in choices:
                grouped[key].append(view)

        # Combine views
        new_views = []
        new_view_groups = list(grouped.values())
        for view_group in new_view_groups:
            new_dimensions = view_group[0].dimensions.copy()
            new_dimensions[dimension] = choice_new_slug
            new_view = View(
                dimensions=new_dimensions,
                indicators=_combine_view_indicators(view_group),
                config=config_new,
            )
            new_views.append(new_view)

        return new_views


def _combine_view_indicators(views: List[View]):
    y_indicators = []
    for view in views:
        if view.indicators.has_non_y_indicators():
            raise NotImplementedError(
                "Merging indicators from views is only implemented for views with *only* y indicators."
            )
        if view.indicators.y is None:
            raise ValueError("View must have y indicators to be combined.")
        y_indicators.extend(view.indicators.y)

    indicators = ViewIndicators(y=y_indicators)
    return indicators


def check_duplicate_views(views: List[View]):
    """Check for duplicate views in the collection."""
    seen_dims = set()
    for view in views:
        dims = tuple(view.dimensions.items())
        if dims in seen_dims:
            raise DuplicateCollectionViews(f"Duplicate view:\n\n{yaml.dump(view.dimensions)}")
        seen_dims.add(dims)


def replace_catalog_paths_with_ids(config):
    """Replace special metadata fields with their corresponding IDs in the database.

    In ETL, we allow certain fields in the config file to reference indicators by their catalog path. However, this is not yet supported in the Grapher API, so we need to replace these fields with the corresponding indicator IDs.

    NOTE: I think this is something that we should discuss changing on the Grapher side. So I see this function as a temporary workaround.

    Currently, affected fields are:

    - views[].config.sortColumnSlug

    These fields above are treated like fields in `dimensions`, and also accessed from:
    - `expand_catalog_paths`: To expand the indicator URI to be in its complete form.
    - `validate_multidim_config`: To validate that the indicators exist in the database.

    TODO: There might be other fields which might make references to indicators:
        - config.map.columnSlug
        - config.focusedSeriesNames
    """
    if "views" in config:
        views = config["views"]
        for view in views:
            if "config" in view:
                # Update sortColumnSlug
                if "sortColumnSlug" in view["config"]:
                    # Check if catalogPath
                    # Map to variable ID
                    view["config"]["sortColumnSlug"] = str(map_indicator_path_to_id(view["config"]["sortColumnSlug"]))
                # Update map.columnSlug
                if "map" in view["config"]:
                    if "columnSlug" in view["config"]["map"]:
                        view["config"]["map"]["columnSlug"] = str(
                            map_indicator_path_to_id(view["config"]["map"]["columnSlug"])
                        )

    return config


_pattern = re.compile(r"_([a-z])")


def snake_to_camel(s: str) -> str:
    # Use the compiled pattern to substitute underscores with the uppercase letter.
    return _pattern.sub(lambda match: match.group(1).upper(), s)


# model.core
def camelize(obj: Any, exclude_keys: Optional[Set[str]] = None) -> Any:
    """
    Recursively converts dictionary keys from snake_case to camelCase, unless the key is in exclude_keys.

    Parameters:
        obj: The object (dict, list, or other) to process.
        exclude_keys: An optional iterable of keys that should not be converted (including nested values).
    """
    exclude_keys = exclude_keys or set()

    if isinstance(obj, dict):
        new_obj: dict[Any, Any] = {}
        for key, value in obj.items():
            # Leave the key unchanged if it's in the exclusion list
            if key in exclude_keys:
                new_obj[key] = value
            else:
                new_obj[snake_to_camel(key)] = camelize(value, exclude_keys)
        return new_obj
    elif isinstance(obj, list):
        return [camelize(item, exclude_keys) for item in obj]
    else:
        return obj

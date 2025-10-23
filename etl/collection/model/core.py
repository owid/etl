"""Model for collections."""

import inspect
import json
import re
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Set, cast

import fastjsonschema
import pandas as pd
import yaml
from owid.catalog.meta import GrapherConfig
from owid.catalog.utils import underscore
from structlog import get_logger
from typing_extensions import Self

from apps.chart_sync.admin_api import AdminAPI
from etl.collection.exceptions import DuplicateCollectionViews, DuplicateValuesError
from etl.collection.model.base import MDIMBase, pruned_json
from etl.collection.model.dimension import Dimension, DimensionChoice
from etl.collection.model.schema_types import (
    GroupViewsConfig,
    ViewConfig,
    ViewConfigParam,
    ViewMetadata,
    ViewMetadataParam,
)
from etl.collection.model.view import CommonView, View, ViewIndicators
from etl.collection.utils import (
    fill_placeholders,
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
    common_views: List[CommonView] | None = None

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

    dependencies: set[str] = field(default_factory=set)
    topic_tags: List[str] | None = None
    _default_dimensions: Dict[str, str] | None = None

    # Internal use. For save() method.
    _collection_type: str | None = field(init=False, default="multidim")
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

        # If dictionary contains field 'definitions', change it for '_definitions'
        if "default_dimensions" in data:
            data["_default_dimensions"] = data["default_dimensions"]
            del data["default_dimensions"]

        # Now that data is in the expected shape, let the parent class handle the rest
        return super().from_dict(data)

    def __post_init__(self):
        # Sanity check
        assert "#" in self.catalog_path, "Catalog path should be in the format `path#name`."

        if isinstance(self.dependencies, list):
            # Convert list to set
            self.dependencies = set(self.dependencies)

    @property
    def definitions(self) -> Definitions:
        return self._definitions

    @property
    def default_dimensions(self) -> Dict[str, str] | None:
        return self._default_dimensions

    @default_dimensions.setter
    def default_dimensions(self, view_dimensions: Dict[str, str]) -> None:
        """Set the default view for the collection.

        Args:
            view_dimensions: Dictionary mapping dimension slugs to their choice values
                representing the view that should be displayed by default.

        Raises:
            ValueError: If the view dimensions don't correspond to any existing view
        """
        # Validate that this view actually exists
        if not isinstance(view_dimensions, dict):
            raise ValueError(f"Cannot set default view to {view_dimensions}: must be of type `dict`")

        found = False
        for view in self.views:
            if all(view.dimensions.get(dim) == choice for dim, choice in view_dimensions.items()):
                found = True
                break

        if not found:
            raise ValueError(f"Cannot set default view to {view_dimensions}: no view matches these dimensions")

        self._default_dimensions = view_dimensions

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
        log.info(f"Exporting collection config to {self.local_config_path}")
        self.save_file(self.local_config_path, force_create=True)

    def save(  # type: ignore[override]
        self,
        owid_env: OWIDEnv | None = None,
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

        # Ensure that all views are in choices
        self.validate_views_with_dimensions()

        # Validate duplicate views
        self.check_duplicate_views()

        # Check that no choice name or slug is repeated
        self.validate_choice_uniqueness()

        # Check that no choice name or slug is repeated
        self.validate_dimension_uniqueness()

        # Validate that datasets used are part of the dependencies
        indicators = self.indicators_in_use(tolerate_extra_indicators)
        self.validate_indicators_are_from_dependencies(indicators)

        # Check that all indicators in collection exist
        validate_indicators_in_db(indicators, owid_env.engine)

        # Run sanity checks on grouped views
        self.validate_grouped_views()

        # Sort views based on dimension order
        self.sort_views_based_on_dimensions()

        # Pick default view first
        self.sort_views_with_default_first()

        # TODO: Prune dimensions if only one choice is in use
        if prune_dimensions:
            self.prune_dimensions()

        # Snake case all slugs (in dimensions and views)
        self.snake_case_slugs()

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

        # Link to preview
        log.info(f"PREVIEW: {owid_env.collection_preview(self.catalog_path)}")

    def snake_case_slugs(self):
        """
        Convert all slugs in dimensions and views to snake_case format.

        This method ensures that all slugs in `self.dimensions` and `self.views` are in snake_case.
        It validates the format of slugs and raises errors if they do not meet the required criteria.

        Input expectations:
        - `self.dimensions` is a list of `Dimension` objects, each with slugs to be converted.
        - `self.views` is a list of `View` objects, each containing dimension and choice slugs.

        Error conditions:
        - Raises `ValueError` if a slug does not match the snake_case format.
        - Raises `ValueError` if a dimension or choice slug is not found in the mappings.
        """

        def _validated_underscore(text):
            if text == "":
                text = "na"
            else:
                text = underscore(text)
            # Validate that the text contains only lowercase letters and underscores
            if not re.match(r"^[a-z][a-z0-9_]*$|^_[a-z0-9][a-z0-9_]*$", text):
                raise ValueError(
                    f"Text '{text}' must start with a lowercase letter or underscore followed by at least one alphanumeric character, and contain only lowercase letters, digits, and underscores."
                )
            return text

        # 1) Build mappings
        dimension_choices = self.dimension_choices_in_use()
        dimension_mapping = {slug: _validated_underscore(slug) for slug in dimension_choices.keys()}
        choice_mapping = {
            dim_slug: {choice_slug: _validated_underscore(choice_slug) for choice_slug in choice_slugs}
            for dim_slug, choice_slugs in dimension_choices.items()
        }

        # 2) Check that all mappings are not repeated (dimension_mapping)
        # Check that all dimension slugs are unique and raise error with duplicates
        def ensure_unique(mapping: Dict[str, str], mapping_name: str):
            if len(set(mapping.values())) != len(mapping):
                duplicates = [
                    slug for slug, count in pd.Series(list(mapping.values())).value_counts().items() if count > 1
                ]
                raise ValueError(
                    f"Duplicate {mapping_name} slugs found: {duplicates}\n\n (note: if 'na', source could be in empty slug)"
                )

        ensure_unique(dimension_mapping, "dimension")
        for dim_slug, choices in choice_mapping.items():
            ensure_unique(choices, f"choice slugs for dimension {dim_slug}")

        # 3) Snake case all slugs in dimensions + choices
        for dim in self.dimensions:
            ## Choice slug
            for choice in dim.choices:
                assert choice.slug in choice_mapping[dim.slug], "Choice slug not found in mapping!"
                choice.slug = choice_mapping[dim.slug][choice.slug]
            ## Dimension slug
            assert dim.slug in dimension_mapping, "Dimension slug not found in mapping!"
            dim.slug = dimension_mapping[dim.slug]
            ## Presentation: choice_slug_true
            if dim.presentation and dim.presentation.choice_slug_true:
                # Check if the choice slug is in the mapping
                if dim.presentation.choice_slug_true not in choice_mapping[dim.slug]:
                    raise ValueError(
                        f"Choice slug {dim.presentation.choice_slug_true} not found in mapping for dimension {dim.slug}!"
                    )
                # Set the new slug
                dim.presentation.choice_slug_true = choice_mapping[dim.slug][dim.presentation.choice_slug_true]

        # 4) Snake case all slugs in views based on the mapping from 1. Raise error if any slug is not found in the mapping.
        for view in self.views:
            view_dimensions = {}
            for dim_slug, choice_slug in view.dimensions.items():
                if dim_slug not in dimension_mapping:
                    raise ValueError(f"Dimension slug {dim_slug} not found in mapping!")
                if choice_slug not in choice_mapping[dim_slug]:
                    raise ValueError(f"Choice slug {choice_slug} not found in mapping for dimension {dim_slug}!")
                # Set the new slugs
                view_dimensions[dimension_mapping[dim_slug]] = choice_mapping[dim_slug][choice_slug]
            # Update dimensions
            view.dimensions = view_dimensions

    def to_dict(self, encode_json: bool = False, drop_definitions: bool = True) -> Dict[str, Any]:  # type: ignore
        dix = super().to_dict(encode_json=encode_json)
        if drop_definitions:
            dix = {k: v for k, v in dix.items() if k not in {"_definitions", "definitions"}}
        return dix

    def get_dimension(self, slug: str) -> Dimension:
        """Get dimension object with slug `slug`"""
        for dim in self.dimensions:
            if dim.slug == slug:
                return dim
        raise ValueError(f"Dimension {slug} not found in dimensions!")

    def get_choice_names(self, dimension_slug: str) -> Dict[str, str]:
        """Get all choice names in a given dimension."""
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

    def validate_schema(self, schema_path: str | Path | None = None):
        """Validate class against schema."""
        if schema_path is None:
            schema_path = self.schema_path
        with open(schema_path) as f:
            s = f.read()

            # Add "file://" prefix to "dataset-schema.json#"
            # This is needed to activate file handler below. Unfortunately, fastjsonschema does not
            # support file references out of the box
            s = s.replace("dataset-schema.json#", "file://dataset-schema.json#")
            s = s.replace("definitions.json#", "file://definitions.json#")

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

    def sort_choices(self, slug_order: Dict[str, List[str] | Callable]):
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

    def sort_views_with_default_first(self):
        # If default dimensions are specified, move that view to the front
        if not self.default_dimensions:
            return

        # Find the default view
        default_view = None
        for view in self.views:
            # Check if this view matches exactly the default dimensions
            if view.dimensions.keys() == self.default_dimensions.keys() and all(
                view.dimensions[dim] == choice for dim, choice in self.default_dimensions.items()
            ):
                default_view = view
                break

        # If no matching view was found, show available options and raise error
        if not default_view:
            df = pd.DataFrame([v.dimensions for v in self.views])
            dimensions_str = "\n".join([f"{k}: {v}" for k, v in self.default_dimensions.items()])
            raise ValueError(
                f"No view matches dimensions:\n\n{dimensions_str}\n\n"
                f"Available dimensions in views are:\n\n{df.to_string()}"
            )

        # Move the default view to the front
        self.views.remove(default_view)
        self.views.insert(0, default_view)

    def validate_choice_uniqueness(self):
        """Validate that all choice names (and slugs) are unique."""
        for dim in self.dimensions:
            dim.validate_choice_names_unique()
            dim.validate_choice_slugs_unique()

    def validate_dimension_uniqueness(self):
        """Validate that all choice names (and slugs) are unique."""
        slugs = set()
        for dim in self.dimensions:
            # Check if slug was already seen
            if dim.slug in slugs:
                raise DuplicateValuesError(
                    f"Dimension slug '{dim.slug}' is not unique! Found in dimensions: {self.dimensions}"
                )

            # Add slug to set
            slugs.add(dim.slug)

    def validate_indicators_are_from_dependencies(self, indicators):
        """Validate that the provided indicators are from tables in datasets specified in the collections dependencies."""
        deps = {dep.split("://", 1)[-1] if "://" in dep else dep for dep in self.dependencies}
        for indicator in indicators:
            if not any(indicator.startswith(f"{dep}/") for dep in deps):
                raise ValueError(f"Indicator {indicator} is not covered by any dependency: {deps}")
        return True

    def validate_grouped_views(self):
        for view in self.views:
            if view.is_grouped:
                sanity_check_grouped_view(view)

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
        dimensions: Dict[str, List[str] | str] | List[Dict[str, List[str] | str]],
    ):
        """Remove views that have any set of dimensions that can be generated from the given in `dimensions`.

        The argument `dimension` can be either a dictionary or a list of dictionaries. Each dictionary represents a set of dimension filters to drop. The keys are the dimension slugs, and the values are either a list of choices or a single choice.

        Depending on the structure of `dimensions`, you can define ANDs and ORs operations. Read the documentation below for examples.

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

    def group_views(
        self,
        groups: List[GroupViewsConfig],  # Also accepts List[Dict[str, Any]] for backward compatibility
        drop_dimensions_if_single_choice: bool = True,
        params: Dict[str, Any] | None = None,
    ):
        """Group views into new ones.

        Group views in a single view to show an aggregate view combining multiple choices for a given dimension. It takes all the views where the `dimension` choice is one of `choices` and groups them together to create a new one.

        Args:
            groups (List[GroupViewsConfig]): List of group configurations with the following keys:
                    - dimension: str
                        Slug of the dimension that contains the choices to group.
                    - choices: List[str]
                        Slugs of the choices to group. If none, all choices are used!
                    - choice_new_slug: str
                        The slug for the newly created choice. If the MDIM config file doesn't specify a name, it will be the same as the slug.
                    - view_config: Optional[Dict[str, Any]], default=None
                        The view config for the new choice. E.g. useful to tweak the chart type.
                    - view_metadata: Optional[Dict[str, Any]], default=None
                        The metadata for the new view. Useful to tweak the metadata around the chart in a data page (e.g. description key, etc.)
                    - replace: Optional[bool], default=False
                        If True, the original choices will be removed and replaced with the new choice. If False, the original choices will be kept and the new choice will be added.
                    - overwrite_dimension_choice: Optional[bool], default=False
                        If True and `choice_new_slug` already exists as a `choice` in `dimension`, views created here will overwrite those already existing if there is any collision.
            drop_dimensions_if_single_choice (bool):
                If True, drop dimensions that always have one choice in use. A dropdown (or dimension) that always is set to a constant value is not really useful, and hence we drop it by default. Default: True. To keep the dropdown, even if just with one option, set this to False.
            params (Dict[str, Any]):
                Optional parameters to pass to the config and metadata. Keys of the dictionary are the parameter names, and values can either be strings or callables. NOTE: Callables must have one argument, which should be the grouped view. See Example 2 below for more details.

        Example 1:
        ----------

        Suppose you have two dimensions

        - sex: 'female', 'male'
        - age: '0-4', '5-9', '10-14'

        Each view shows a single timeseries. Now, you now want to create a new view, with sex="combined", where the user can observe both timeseries (female and male) in a single view. Hence you'd end up with the following choices:

            sex: 'female', 'male', 'combined'
            age: '0-4', '5-9', '10-14'


        In this example, you should use method arguments as `dimension="sex"`, `choices=["female", "male"]`, and `choice_new_slug="combined"`.

        Example 2:
        ----------
        Sometimes, you may want to define config for the new views. In the example above, you have generated new views for male+female in three different age brackets. Suppose that you want to set the titles each of the three new views: "Population of people aged 0-4", "Population of people aged 5-9", and "Population of people aged 0-4 and 5-9", respectively.

        You can programmatically do this with `view_config` and `params` (see code snippets below). Basically, you need to define a title template in `view_config`, and then pass the parameters in `params`. The template will be filled with the values in `params`.

        ```python
        c.group_views(
            groups=[
                {
                    "dimension": "sex",
                    "view_config": {
                        "title": "Population of people aged {age}",
                    }
                }
            ],
            params={
                "age": "0-4",
            }
        )
        ```

        You can also use a function to dynamically generate a parameter:

        ```python
        c.group_views(
            groups=[
                {
                    "dimension": "sex",
                    "view_config": {
                        "title": "Population of people aged {age}",
                    },
                }
            ],
            "params": {
                "age": lambda view: view.dimensions["age"],
            },
        )
        ```
        """

        def _ensure_choices(group, dimension):
            # Get choice slugs
            if "choices" not in group:
                return self.get_dimension(dimension).choice_slugs
            else:
                return group["choices"]

        new_views_all = []
        for group in groups:
            # Get dimension slug
            assert "dimension" in group, "Dimension must be provided!"
            dimension = group["dimension"]
            # Get choice slugs
            choices = _ensure_choices(group, dimension)

            # Get new choice slug
            assert "choice_new_slug" in group, "Dimension must be provided!"
            choice_new_slug = group["choice_new_slug"]

            # Config of new views
            view_config = group.get("view_config")
            view_metadata = group.get("view_metadata")

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
                view_config=view_config,
                view_metadata=view_metadata,
                params=params,
            )
            new_views_all.append(
                {
                    "overwrite": group.get("overwrite_dimension_choice", False),
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
        for group in groups:
            if group.get("replace", False):
                dimension = group["dimension"]
                choices = _ensure_choices(group, dimension)
                # Remove views with old choices
                new_views = [view for view in self.views if view.dimensions[dimension] not in choices]
                # Remove unused choices
                self.prune_dimension_choices()

        # Drop dimension if it has only one choice in use
        if drop_dimensions_if_single_choice:
            self.prune_dimensions()

    def set_global_config(
        self,
        config: ViewConfigParam,
        params: Dict[str, Any] | None = None,
    ):
        self.edit_views(
            [
                # General
                {
                    "config": config,
                }
            ],
            params=params,
        )

    def set_global_metadata(
        self,
        metadata: ViewMetadataParam,
        params: Dict[str, Any] | None = None,
    ):
        self.edit_views(
            [
                # General
                {
                    "metadata": metadata,
                }
            ],
            params=params,
        )

    def edit_views(
        self,
        edits: List[Dict[str, Any]],
        params: Dict[str, Any] | None = None,
    ):
        """Edit the display of a view. Text can come from `config` (Grapher config) or `metadata` (Grapher metadata, i.e. text in the data page).

        Args:
            edits (List[Dict[str, Any]]): List of dictionaries with the following keys
                - dimensions: Dict[str, str]
                    Slugs of the dimensions to edit. The keys are the dimension slugs, and the values are the new choice slugs.
                - config: Dict[str, Any]
                    The config for the new choice. E.g. useful to tweak the chart type.
                - metadata: Dict[str, Any]
                    The metadata for the new choice. E.g. useful to tweak the metadata around the chart in a data page (e.g. description key, etc.)
            params (Dict[str, Any]): Optional parameters to pass to the config and metadata. Keys of the dictionary are the parameter names, and values can either be strings or callables. NOTE: Callables must have one argument, which should be the grouped view. See Example 2 below for more details.
        """
        # Check edits is a list of dicts
        if not isinstance(edits, list):
            raise TypeError("Edits must be a list of dictionaries!")
        if not all(isinstance(edit, dict) for edit in edits):
            raise TypeError("Edits must be a list of dictionaries!")

        # Create CommonView objects
        common_views = []
        for edit in edits:
            ## Create common view
            cv = CommonView(**edit)
            common_views.append(cv)

        # Apply common views (with priority over view-specific config)
        ## NOTE: we allow to fill in a view with a parametrized config and metadata. We then make sure to fill in the parameters. We do it this way to avoid touching `combine_with_common` method.
        for view in self.views:
            ## Combine with common views
            view.combine_with_common(common_views, common_has_priority=True)
            ## TODO: Fill in possible params
            view = _set_config_metadata_with_params(view, view.config, view.metadata, params)

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

        # Check that the new choice slug is not IN USE. Note that it could still be in the dimension choices, but not in use. NOTE: this is tricky. As implemented above it fails for war/latest/mars collection step.
        # choices_in_use = self.dimension_choices_in_use()
        # if choice_new_slug in choices_in_use[dimension]:
        #     raise ValueError(
        #         f"Choice slug `{choice_new_slug}` already exists in dimension {dimension}! Available choices are: {dimension_choices[dimension]}"
        #     )

    def create_new_grouped_views(
        self,
        dimension: str,
        choices: List[str],
        choice_new_slug: str,
        view_config: ViewConfigParam | None = None,
        view_metadata: ViewMetadataParam | None = None,
        params: Dict[str, Any] | None = None,
    ) -> List[View]:
        """Create new grouped views."""
        if params is None:
            params = {}

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
            # Create dimensions for new view
            new_dimensions = view_group[0].dimensions.copy()
            new_dimensions[dimension] = choice_new_slug
            # Create new view
            new_view = View(
                dimensions=new_dimensions,
                indicators=_combine_view_indicators(view_group),
            )
            # Mark as grouped view
            new_view.mark_as_grouped()

            # Create config for new view
            new_view = _set_config_metadata_with_params(new_view, view_config, view_metadata, params)

            # Add new view to list
            new_views.append(new_view)

        return new_views


def _expand_params(params: Dict[str, Any], view: View) -> Dict[str, Any]:
    """Expand parameters in the config and metadata."""
    # Create config for new view
    params_view = params.copy()
    for p, k in params_view.items():
        if isinstance(k, Callable):
            params_view[p] = k(view)
    return params_view


def _set_config_metadata_with_params(
    view,
    view_config: ViewConfigParam | GrapherConfig | None = None,
    view_metadata: ViewMetadataParam | Any | None = None,
    params: Dict[str, Any] | None = None,
) -> View:
    # Set params to dict if None
    if params is None:
        params = {}

    # Sanity check
    if (view_config is None) and (view_metadata is None):
        return view
        # raise ValueError("Either view_config or view_metadata must be provided!")

    # Execute callables in params to get a proper Dict[str, str]
    params_view = _expand_params(params, view)

    # Get config and metadata filled with params
    new_config = fill_placeholders(view_config, params_view) if view_config else None
    new_metadata = fill_placeholders(view_metadata, params_view) if view_metadata else None

    # Run callbacks on config and metadata
    new_config = run_callbacks(new_config, view)
    new_metadata = run_callbacks(new_metadata, view)

    # Add config and metadata to new view
    # For now, keep as dicts to maintain compatibility with existing merge logic
    view.config = cast(ViewConfig, new_config)
    view.metadata = cast(ViewMetadata, new_metadata)

    return view


def _combine_view_indicators(views: List[View]):
    y_indicators = []
    for view in views:
        if view.indicators.has_non_y_indicators():
            raise NotImplementedError(
                "Merging indicators from views is only implemented for views with *only* y indicators."
            )
        if view.indicators.y is None:
            raise ValueError("View must have y indicators to be combined.")
        y_indicators.extend(deepcopy(view.indicators.y))

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
def camelize(obj: Any, exclude_keys: Set[str] | None = None) -> Any:
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


def run_callbacks(data, view):
    """Run callbacks on the data."""
    if data is None:
        return data

    if isinstance(data, dict):
        return {k: run_callbacks(v, view) for k, v in data.items()}

    if isinstance(data, (list, tuple, set)):
        container_type = type(data)
        return container_type(run_callbacks(item, view) for item in data)

    if inspect.isfunction(data):
        # All placeholders are present – safe to format
        return data(view)
    # Otherwise, return the data as is
    return data


def sanity_check_grouped_view(view: View) -> None:
    """
    Perform sanity checks on grouped views.

    Validates that grouped views have proper metadata configuration,
    specifically checking for required description fields.

    Args:
        view: The grouped view to validate

    Warns:
        UserWarning: If required metadata fields are missing
    """
    import warnings

    # Check if metadata is defined
    if view.metadata is None:
        warnings.warn(
            f"Grouped view with dimensions {view.dimensions} is missing 'metadata' attribute. "
            "Consider adding metadata with 'description_key' and 'description_short' fields.",
            UserWarning,
            stacklevel=2,
        )
        return

    # Convert to dict if it's a ViewMetadata object for easier checking
    metadata_dict = (
        view.metadata
        if isinstance(view.metadata, dict)
        else view.metadata.to_dict()
        if hasattr(view.metadata, "to_dict")
        else {}
    )

    # Check for description_key
    if "description_key" not in metadata_dict or metadata_dict["description_key"] is None:
        warnings.warn(
            f"Grouped view with dimensions {view.dimensions} is missing 'description_key' in metadata. "
            "This field provides key information about the grouped view.",
            UserWarning,
            stacklevel=2,
        )

    # Check for description_short
    if "description_short" not in metadata_dict or metadata_dict["description_short"] is None:
        warnings.warn(
            f"Grouped view with dimensions {view.dimensions} is missing 'description_short' in metadata. "
            "This field provides a short description of the grouped view.",
            UserWarning,
            stacklevel=2,
        )

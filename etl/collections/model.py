"""WIP: Drafting a model for dealing with MDIM/Explorer configuration.

This should be aligned with the MDIM schema.

THINGS TO SOLVE:

    - If an attribute is Optional, MetaBase.from_dict is not correctly loading it as the appropriate class when given.
"""

import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, ClassVar, Dict, List, Optional, Set, TypeGuard, TypeVar, Union

import fastjsonschema
import pandas as pd
import yaml
from owid.catalog.meta import GrapherConfig, MetaBase
from structlog import get_logger
from typing_extensions import Self

from etl.collections.exceptions import DuplicateCollectionViews
from etl.collections.utils import merge_common_metadata_by_dimension, validate_indicators_in_db
from etl.config import OWID_ENV, OWIDEnv
from etl.files import yaml_dump
from etl.paths import EXPORT_DIR, SCHEMAS_DIR

# Initialize logger.
log = get_logger()


CHART_DIMENSIONS = ["y", "x", "size", "color"]
T = TypeVar("T")
REGEX_CATALOG_PATH = (
    r"^grapher/[A-Za-z0-9_]+/(?:\d{4}-\d{2}-\d{2}|\d{4}|latest)/[A-Za-z0-9_]+/[A-Za-z0-9_]+#[A-Za-z0-9_]+$"
)
REGEX_CATALOG_PATH_OPTIONS = (
    r"^(?:(?:grapher/[A-Za-z0-9_]+/(?:\d{4}-\d{2}-\d{2}|\d{4}|latest)/)?[A-Za-z0-9_]+/)?[A-Za-z0-9_]+#[A-Za-z0-9_]+$"
)


def prune_dict(d: dict) -> dict:
    """Remove all keys starting with underscore and all empty values from a dictionary.

    NOTE: This method was copied from owid.catalog.utils. It is slightly different in the sense that it does not remove fields with empty lists! This is because there are some fields which are mandatory and can be empty! (TODO: should probably fix the schema / engineering side)

    """
    out = {}
    for k, v in d.items():
        if not k.startswith("_") and v not in [None, {}]:
            if isinstance(v, dict):
                out[k] = prune_dict(v)
            elif isinstance(v, list):
                out[k] = [prune_dict(x) if isinstance(x, dict) else x for x in v if x not in [None, {}]]
            else:
                out[k] = v
    return out


def pruned_json(cls: T) -> T:
    orig = cls.to_dict  # type: ignore

    # only keep non-null public variables
    # calling original to_dict returns dictionaries, not objects
    cls.to_dict = lambda self, **kwargs: prune_dict(orig(self, **kwargs))  # type: ignore

    return cls


class MDIMBase(MetaBase):
    def save_file(self, filename: Union[str, Path]) -> None:
        filename = Path(filename).as_posix()
        with open(filename, "w") as ostream:
            json.dump(self.to_dict(), ostream, indent=2, default=str)


@pruned_json
@dataclass
class Indicator(MDIMBase):
    catalogPath: str
    display: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        # Validate that the catalog path is either (i) complete or (ii) in the format table#indicator.
        if not self.is_a_valid_path(self.catalogPath):
            raise ValueError(f"Invalid catalog path: {self.catalogPath}")

    def has_complete_path(self) -> bool:
        pattern = re.compile(REGEX_CATALOG_PATH)
        complete = bool(pattern.match(self.catalogPath))
        return complete

    @classmethod
    def is_a_valid_path(cls, path: str) -> bool:
        """Valid paths are:
        - grapher/namespace/version/dataset/table#indicator.
        - dataset/table#indicator
        - table#indicator
        """
        pattern = re.compile(REGEX_CATALOG_PATH_OPTIONS)
        valid = bool(pattern.match(path))
        return valid

    def __setattr__(self, name, value):
        """Validate that the catalog path is either (i) complete or (ii) in the format table#indicator."""
        if hasattr(self, name):
            if (name == "catalogPath") and (not self.is_a_valid_path(value)):
                raise ValueError(f"Invalid catalog path: {value}")
        return super().__setattr__(name, value)

    def expand_path(self, tables_by_name: Dict[str, List[str]]):
        # Do nothing if path is already complete
        if self.has_complete_path():
            return self

        # If path is not complete, we need to expand it!
        table_name, indicator_name = self.catalogPath.split("#")

        # Check table is in any of the datasets!
        assert (
            table_name in tables_by_name
        ), f"Table name `{table_name}` not found in dependency tables! Available tables are: {', '.join(tables_by_name.keys())}"

        # Check table name to table mapping is unique
        assert (
            len(tables_by_name[table_name]) == 1
        ), f"There are multiple dependencies (datasets) with a table named {table_name}. Please add dataset name (dataset_name/table_name#indicator_name) if you haven't already, or use the complete dataset URI in this case."

        # Check dataset in table metadata is not None
        tb_uri = tables_by_name[table_name][0]
        # assert tb.m.dataset is not None, f"Dataset not found for table {table_name}"

        # Build URI
        self.catalogPath = tb_uri + "#" + indicator_name

        return self


@pruned_json
@dataclass
class ViewIndicators(MDIMBase):
    """Indicators in a MDIM/Explorer view."""

    y: Optional[List[Indicator]] = None
    x: Optional[Indicator] = None
    size: Optional[Indicator] = None
    color: Optional[Indicator] = None

    @property
    def num_indicators(self) -> int:
        """Get the total number of indicators in the view."""
        return sum([1 for dim in CHART_DIMENSIONS if getattr(self, dim, None) is not None])

    def has_non_y_indicators(self) -> bool:
        """Check if the view has non-y indicators."""
        return any([getattr(self, dim, None) is not None for dim in CHART_DIMENSIONS[1:]])

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ViewIndicators":
        """Coerce the dictionary into the expected shape before passing it to the parent class."""
        # Make a shallow copy so we don't mutate the user's dictionary in-place
        data = dict(d)

        # Coerce each dimension field (y, x, size, color) from [str, ...] -> [{'path': str}, ...]
        for dim in CHART_DIMENSIONS:
            if dim in data:
                if isinstance(data[dim], list):
                    data[dim] = [{"catalogPath": item} if isinstance(item, str) else item for item in data[dim]]
                else:
                    if isinstance(data[dim], str):
                        data[dim] = [{"catalogPath": data[dim]}] if dim == "y" else {"catalogPath": data[dim]}
                    elif dim == "y":
                        data[dim] = [data[dim]]
        # Now that data is in the expected shape, let the parent class handle the rest
        return super().from_dict(data)

    def to_records(self) -> List[Dict[str, Union[str, Dict[str, Any]]]]:
        indicators = []
        for dim in CHART_DIMENSIONS:
            dimension_val = getattr(self, dim, None)
            if dimension_val is None:
                continue
            if isinstance(dimension_val, list):
                for d in dimension_val:
                    display = d.display if d.display is not None else {}
                    indicator_ = {"path": d.catalogPath, "axis": dim, "display": display}
                    indicators.append(indicator_)
            else:
                display = dimension_val.display if dimension_val.display is not None else {}
                indicator_ = {
                    "path": dimension_val.catalogPath,
                    "axis": dim,
                    "display": display,
                }
                indicators.append(indicator_)
        return indicators

    def expand_paths(self, tables_by_name: Dict[str, List[str]]):
        """Expand the catalog paths of all indicators in the view."""
        for dim in CHART_DIMENSIONS:
            dimension_val = getattr(self, dim, None)
            if dimension_val is None:
                continue
            if isinstance(dimension_val, list):
                for indicator in dimension_val:
                    indicator.expand_path(tables_by_name)
            else:
                dimension_val.expand_path(tables_by_name)

        return self


@pruned_json
@dataclass
class CommonView(MDIMBase):
    dimensions: Optional[Dict[str, Any]] = None
    config: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None

    @property
    def num_dimensions(self) -> int:
        return len(self.dimensions) if self.dimensions is not None else 0


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
class View(MDIMBase):
    """MDIM/Explorer view configuration."""

    dimensions: Dict[str, str]
    indicators: ViewIndicators
    # NOTE: Maybe worth putting as classes at some point?
    config: Optional[GrapherConfig] = None
    metadata: Optional[Any] = None

    @property
    def d(self):
        return self.dimensions

    def has_non_y_indicators(self) -> bool:
        """Check if the view has non-y indicators."""
        return self.indicators.has_non_y_indicators()

    @property
    def has_multiple_indicators(self) -> bool:
        # Get list of indicators
        indicators = self.indicators.to_records()
        return len(indicators) > 1

    @property
    def num_indicators(self) -> int:
        """Get the total number of indicators in the view."""
        return self.indicators.num_indicators

    @property
    def metadata_is_needed(self) -> bool:
        return self.has_multiple_indicators and (self.metadata is None)

    def expand_paths(self, tables_by_name: Dict[str, List[str]]):
        """Expand all indicator paths in the view.

        Make sure that they are all complete paths. This includes indicators in view, but also those in config (if any).
        """
        # Expand paths in indicators
        self.indicators.expand_paths(tables_by_name)

        # Expand paths in config fields
        if self.config is not None:
            if "sortColumnSlug" in self.config:
                indicator = Indicator(self.config["sortColumnSlug"]).expand_path(tables_by_name)
                self.config["sortColumnSlug"] = indicator.catalogPath

            if "map" in self.config:
                if "columnSlug" in self.config["map"]:
                    indicator = Indicator(self.config["map"]["columnSlug"]).expand_path(tables_by_name)
                    self.config["map"]["columnSlug"] = indicator.catalogPath

        return self

    def combine_with_common(self, common_views: List[CommonView]):
        """Combine config and metadata fields in view with those specified by definitions.common_views."""
        # Update config
        new_config = merge_common_metadata_by_dimension(common_views, self.dimensions, self.config, "config")
        if new_config:
            self.config = new_config
        # Update metadata
        new_metadata = merge_common_metadata_by_dimension(common_views, self.dimensions, self.metadata, "metadata")
        if new_metadata:
            self.metadata = new_metadata

        return self

    @property
    def indicators_in_config(self):
        indicators = []
        if self.config is not None:
            # Get indicators from sortColumnSlug
            if "sortColumnSlug" in self.config:
                indicators.append(self.config["sortColumnSlug"])

            # Update indicators from map.columnSlug
            if ("map" in self.config) and "columnSlug" in self.config["map"]:
                indicators.append((self.config["map"]["columnSlug"]))

        return indicators

    def indicators_used(self, tolerate_extra_indicators: bool = False):
        """Get a flatten list of all indicators used in the view.

        In addition, it also validates that indicators used in config are also in the view.

        NOTE: Use this method after expanding paths! Otherwise, it will not work as expected. E.g. view.expand_paths(tables_by_name).indicators_used()
        """
        # Validate indicators in view
        indicators = self.indicators.to_records()
        indicators = [ind["path"] for ind in indicators]

        # All indicators in `indicators_extra` should be in `indicators`! E.g. you can't sort by an indicator that is not in the chart!
        ## E.g. the indicator used to sort, should be in use in the chart! Or, the indicator in the map tab should be in use in the chart!
        invalid_indicators = set(self.indicators_in_config).difference(set(indicators))
        if not tolerate_extra_indicators and invalid_indicators:
            raise ValueError(
                f"Extra indicators not in use. This means that some indicators are referenced in the chart config (e.g. map.columnSlug or sortColumnSlug), but never used in the chart tab. Unexpected indicators: {invalid_indicators}. If this is expected, set `tolerate_extra_indicators=True`."
            )
        elif invalid_indicators:
            indicators = indicators + list(invalid_indicators)

        return indicators


# @dataclass
# class ExplorerView(View):
#     """https://github.com/owid/owid-grapher/blob/cb01ebb366d22f255b0acb791347981867225e8b/packages/%40ourworldindata/explorer/src/GrapherGrammar.ts"""

#     pass


# @dataclass
# class MDIMView(View):
#     pass


@pruned_json
@dataclass
class DimensionChoice(MDIMBase):
    slug: str
    name: str
    description: Optional[str] = None


@dataclass(frozen=True)
class UITypes:
    DROPDOWN: ClassVar[str] = "dropdown"
    CHECKBOX: ClassVar[str] = "checkbox"
    RADIO: ClassVar[str] = "radio"
    TEXT_AREA: ClassVar[str] = "text_area"  # Adding a new type automatically works!

    # Compute the list once at class definition time
    ALL: ClassVar[List[str]] = [
        value for key, value in vars().items() if not key.startswith("__") and isinstance(value, str)
    ]

    @classmethod
    def is_valid(cls, value: str) -> TypeGuard[str]:
        return value in cls.ALL


@pruned_json
@dataclass
class DimensionPresentation(MDIMBase):
    type: str
    choice_slug_true: Optional[str] = None

    def __post_init__(self):
        if not UITypes.is_valid(self.type):
            raise ValueError(f"Invalid type: {self.type}. Accepted values: {UITypes.ALL}")
        if (self.type == UITypes.CHECKBOX) and (self.choice_slug_true is None):
            raise ValueError(f"`choice_slug_true` slug must be provided for '{UITypes.CHECKBOX}' type.")


@pruned_json
@dataclass
class Dimension(MDIMBase):
    """MDIM/Explorer dimension configuration."""

    slug: str
    name: str
    choices: List[DimensionChoice]
    presentation: Optional[DimensionPresentation] = None

    def __post_init__(self):
        """Validations."""

        # Checks when presentation is checkbox
        if self.ui_type == UITypes.CHECKBOX:
            assert self.presentation is not None, "Presentation must be provided for 'checkbox' type."

            # Choices must be exactly two
            if (num_choices := len(self.choice_slugs)) != 2:
                raise ValueError(
                    f"Dimension choices for '{UITypes.CHECKBOX}' must have exactly two choices. Instead, found {num_choices} choices."
                )

            # True slug must be provided, and must be a valid choice
            # assert self.presentation.choice_slug_true is not None
            if self.presentation.choice_slug_true not in self.choice_slugs:
                raise ValueError(f"True slug '{self.presentation.choice_slug_true}' must be one of the choices.")

    @property
    def ui_type(self):
        if self.presentation is None:
            return UITypes.DROPDOWN
        return self.presentation.type

    @property
    def choice_slugs(self) -> List[str]:
        # if self.choices is not None:
        return [choice.slug for choice in self.choices]

    @property
    def ppt(self):
        return self.presentation

    def sort_choices(self, slug_order: Union[List[str], Callable]):
        """Sort choices based on the given order.

        Args:
        slug_order: List[str] | Callable
            If a list, it must contain all the slugs in the desired order. If a callable, this callable will be applied to the choice slugs to sort them.
        """
        choice_slugs = self.choice_slugs
        if callable(slug_order):
            slug_order_ = slug_order(choice_slugs)
        else:
            slug_order_ = slug_order

        # Make sure all choices are in the given order
        choices_missing = set(choice_slugs) - set(slug_order_)
        if choices_missing:
            raise ValueError(
                f"All choices for dimension {self.slug} must be in the given order! Missing: {choices_missing}"
            )

        # Create a dictionary to map slugs to their positions for faster sorting
        slug_position = {slug: index for index, slug in enumerate(slug_order_)}

        # Sort based on your desired slug order
        self.choices.sort(key=lambda choice: slug_position.get(choice.slug, float("inf")))

    def validate_unique_names(self):
        """Validate that all choice names are unique."""
        names = [choice.name for choice in self.choices]
        if len(names) != len(set(names)):
            raise ValueError(f"Dimension choices for '{self.slug}' must have unique names!")

    def validate_unique_slugs(self):
        """Validate that all choice names are unique."""
        slug = [choice.slug for choice in self.choices]
        if len(slug) != len(set(slug)):
            raise ValueError(f"Dimension choices for '{self.slug}' must have unique names! Review {self.choice_slugs}")


T = TypeVar("T")


@pruned_json
@dataclass
class Collection(MDIMBase):
    """Overall MDIM/Explorer config"""

    dimensions: List[Dimension]
    views: List[View]
    catalog_path: str

    _definitions: Definitions

    # Private for fast access
    # _views_hash: Optional[Dict[str, Any]] = None
    # _dimensions_hash: Optional[Dict[str, Dimension]] = None

    # Internal use. For save() method.
    _collection_type: Optional[str] = field(init=False, default=None)

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
        # with open(self.local_config_path, "w") as f:
        #     yaml_dump(config, f)

    # def save(self):  # type: ignore[override]
    #     pass

    def save(  # type: ignore[override]
        self, owid_env: Optional[OWIDEnv] = None, tolerate_extra_indicators: bool = False, prune_dimensions: bool = True
    ):
        # Ensure we have an environment set
        if owid_env is None:
            owid_env = OWID_ENV

        # Prune non-used dimensions
        if prune_dimensions:
            self.prune_dimension_choices()

        # Check that no choice name is repeated
        self.validate_choice_uniqueness()

        # Check that all indicators in explorer exist
        indicators = self.indicators_in_use(tolerate_extra_indicators)
        validate_indicators_in_db(indicators, owid_env.engine)

        # Export config to local directory in addition to uploading it to MySQL for debugging.
        self.save_config_local()

        # Upsert to DB
        self.upsert_to_db(owid_env)

    def upsert_to_db(self, owid_env: OWIDEnv):
        raise NotImplementedError("Upser to DB must be implemented in children classes!")

    def to_dict(self, encode_json: bool = False, drop_definitions: bool = True) -> Dict[str, Any]:  # type: ignore
        dix = super().to_dict(encode_json=encode_json)
        if drop_definitions:
            dix = {k: v for k, v in dix.items() if k not in {"_definitions", "definitions"}}
        return dix

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
        seen_dims = set()
        for view in self.views:
            dims = tuple(view.dimensions.items())
            if dims in seen_dims:
                raise DuplicateCollectionViews(f"Duplicate view:\n\n{yaml.dump(view.dimensions)}")
            seen_dims.add(dims)

        # NOTE: this is allowed, some views might contain other views
        # Check uniqueness
        # inds = pd.Series(indicators)
        # vc = inds.value_counts()
        # if vc[vc > 1].any():
        #     raise ValueError(f"Duplicate indicators: {vc[vc > 1].index.tolist()}")

    def sort_choices(self, slug_order: Dict[str, Union[List[str], Callable]]):
        """Sort choices based on the given order."""
        for dim in self.dimensions:
            if dim.slug in slug_order:
                dim.sort_choices(slug_order[dim.slug])

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

    def group_views(self, params: List[Dict[str, Any]], drop_dimensions_if_single_choice: bool = True):
        """Group views into new ones.

        Group views in a single view to show an aggregate view combining multiple choices for a given dimension. It takes all the views where the `dimension` choice is one of `choices` and groups them together to create a new one.

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

        Args:
        -----
        params: List[Dict[str, Any]]
            List of dictionaries with the following keys:
                - dimension: str
                    Slug of the dimension that contains the choices to group.
                - choices: List[str]
                    Slugs of the choices to group.
                - choice_new_slug: str
                    The slug for the newly created choice. If the MDIM config file doesn't specify a name, it will be the same as the slug.
                - config_new: Dict[str, Any]
                    The config for the new choice. E.g. useful to tweak the chart type.
                - replace: bool
                    If True, the original choices will be removed and replaced with the new choice. If False, the original choices will be kept and the new choice will be added.
        """
        new_views = []
        for p in params:
            assert "dimension" in p, "Dimension must be provided!"
            assert "choices" in p, "Dimension must be provided!"
            assert "choice_new_slug" in p, "Dimension must be provided!"
            dimension = p["dimension"]
            choices = p["choices"]
            choice_new_slug = p["choice_new_slug"]
            config_new = p.get("config_new", None)

            # Sanity checks
            self._sanity_check_view_grouping(
                dimension,
                choices,
                choice_new_slug,
            )

            # Create new views
            new_views_ = self.create_new_grouped_views(
                dimension=dimension,
                choices=choices,
                choice_new_slug=choice_new_slug,
                config_new=config_new,
            )
            new_views.extend(new_views_)

            # Add dimensions. TODO: Use combine_dimensions instead?
            for dim in self.dimensions:
                if (dim.slug == dimension) and (choice_new_slug not in dim.choice_slugs):
                    new_choice = DimensionChoice(slug=choice_new_slug, name=choice_new_slug)
                    # Add new choice to dimension
                    dim.choices.append(new_choice)
                    break

        # Extend views
        self.views.extend(new_views)

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

    def group_views_single(
        self,
        dimension: str,
        choices: List[str],
        choice_new_slug: str,
        config_new: Optional[GrapherConfig] = None,
        replace: bool = False,
        drop_dimensions_if_single_choice: bool = True,
    ):
        """Group views in a single view.

        Group views in a single view to show an aggregate view combining multiple choices for a given dimension. It takes all the views where the `dimension` choice is one of `choices` and groups them together to create a new one.

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

        Args:
        -----

        dimension: str
            Slug of the dimension that contains the choices to group.
        choices: List[str]
            Slugs of the choices to group.
        choice_new_slug: str
            The slug for the newly created choice. If the MDIM config file doesn't specify a name, it will be the same as the slug.
        config_new: Dict[str, Any]
            The config for the new choice. E.g. useful to tweak the chart type.
        replace: bool
            If True, the original choices will be removed and replaced with the new choice. If False, the original choices will be kept and the new choice will be added.
        """
        if self._group_operations_done > 0:
            log.warning(
                "If you are doing more than one group operation, consider using `group_views` instead. It is optimized for batch operations, where each grouping is done in parallel."
            )

        # Sanity checks
        self._sanity_check_view_grouping(dimension, choices, choice_new_slug)

        # Create new views
        new_views = self.create_new_grouped_views(
            dimension=dimension,
            choices=choices,
            choice_new_slug=choice_new_slug,
            config_new=config_new,
        )

        # Add views to object
        self.views.extend(new_views)

        for dim in self.dimensions:
            if (dim.slug == dimension) and (choice_new_slug not in dim.choice_slugs):
                new_choice = DimensionChoice(slug=choice_new_slug, name=choice_new_slug)
                # Add new choice to dimension
                dim.choices.append(new_choice)
                break

        # Remove original choices if asked to
        if replace:
            # Remove views with old choices
            self.views = [view for view in self.views if view.dimensions[dimension] not in choices]
            # Remove unused choices
            self.prune_dimension_choices()

        # Remove dimension if it has only one choice in use
        if drop_dimensions_if_single_choice:
            self.prune_dimensions()

        self._group_operations_done += 1

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

        # Check that the new choice slug is not already in use
        if choice_new_slug in self.dimension_choices_in_use():
            raise ValueError(
                f"Choice slug {choice_new_slug} in dimension {dimension} already in use by at least one view! Please use another one!"
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


def _combine_view_indicators(views):
    y_indicators = []
    for view in views:
        if view.indicators.has_non_y_indicators():
            raise NotImplementedError(
                "Merging indicators from views is only implemented for views with *only* y indicators."
            )
        y_indicators.extend(view.indicators.y)

    indicators = ViewIndicators(y=y_indicators)
    return indicators

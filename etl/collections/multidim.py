"""TODO:

- Structure MDIM config and related objects in a more pythonic way (e.g. dataclass).
- Need to be able to quickly validate the configs against schemas.
- We should try to keep explorers in mind, and see this tooling as something we may want to use for them, too.
"""

from copy import deepcopy
from typing import Any, Dict, List, Optional, Set, Union

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from apps.chart_sync.admin_api import AdminAPI
from etl.collections.common import map_indicator_path_to_id, validate_collection_config
from etl.collections.model import Multidim
from etl.collections.utils import (
    get_tables_by_name_mapping,
    records_to_dictionary,
)
from etl.config import OWID_ENV, OWIDEnv
from etl.helpers import PathFinder
from etl.paths import SCHEMAS_DIR

# Initialize logger.
log = get_logger()
# Dimensions: These are the expected possible dimensions
CHART_DIMENSIONS = ["y", "x", "size", "color"]
# Base
INDICATORS_SLUG = "indicator"


# TODO: Return List[Dimensions] and List[Views] instead of {"dimensions": [...], "views": [...]}
def expand_config(
    tb: Table,
    indicator_names: Optional[Union[str, List[str]]] = None,
    dimensions: Optional[Union[List[str], Dict[str, Union[List[str], str]]]] = None,
    common_view_config: Optional[Dict[str, Any]] = None,
    indicators_slug: str = INDICATORS_SLUG,
) -> Dict[str, Any]:
    """Create partial config (dimensions and views) from multi-dimensional indicator in table `tb`.

    This method returns the configuration generated from the table `tb`. You can select a subset of indicators with argument `indicator_names`, otherwise all indicators will be expanded.

    Also, it will expand all dimensions and their values, unless `dimensions` is provided. To tweak which dimensions or dimension values are expanded use argument `dimensions` (see below).

    There is also the option to add a common configuration for all views using `common_view_config`. In the future, it'd be nice to support more view-specific configurations. For now, if that's what you want, consider tweaking the output partial config or working on the input indicator metadata (e.g. tweak `grapher_config.title`).

    NOTE
    ----
    1) For more details, refer to class MDIMConfigExpander.

    2) This function generates PARTIAL configuration, you need to then combine it with the config loaded from a YAML file. You can do this combination as you consider. Currently this is mostly manual, but we can look into improving this space:

        ```python
        config = paths.load_mdim_config("filename.yml)
        config_new = expand_config(tb=tb)
        config["views"] = config_new["views"]
        config["dimensions"] = config_new["dimensions"]

        multidim.upsert_multidim_data_page(...)
        ```

    HOWEVER, there is a helper function `combine_config_dimensions` that can help you with combining dimensions.


    3) List of future improvement candidates:
        - Add unit testing.
        - Out-of-the box sorting for dimensions
            - Example: This could be alphabetically ascending or descending.
            - IDEA: We could do this by passing string values directly to dimensions, e.g. dimensions='alphabetical_desc'
        - Out-of-the box sorting for dimension values.
            - Example: This could be alphabetically ascending or descending, or numerically ascending or descending.
            - IDEA: We could pass strings as values directly to the keys in dimensions dictionary, e.g. `dimensions={"sex": "alph_desc", "age": "numerical_desc", "cause": ["aids", "cancer"]}`. To some extent, we already support the function "*" (i.e. show all values without sorting).
        - Support using charts with 'x', 'size' and 'color' indicators. Also support display settings for each indicator.

    Parameters:
    -----------
    tb : Table
        Table with the data, including the indicator and its dimensions. The columns in the table are assumed to contain dimensional information. This can be checked in `tb[col].metadata.additional_info["dimensions"]`.
    indicator_names : str | None
        Name of the indicator to use. This is the actual indicator name, and not the indicator-dimension composite name. If None, it assumes there is only one indicator (and will use it), otherwise it will fail.
    dimensions : None | List[str] | Dict[str, List[str] | str]
        This parameter accepts three types:
            - None:
                - By default, all dimensions and their values are used.
                - The order of dropdowns in the MDIM page (and their values) will be arbitrary.
            - List[str]:
                - The order of dropdowns in the MDIM page will follow the order of the list.
                - If any dimension is missing from the list, this function will raise an error.
                - The order of the dimension values in each dropdown will be arbitrary.
            - Dict[str, str | List[str]]:
                - Keys represent the dimensions, and values are the set of choices to consider for each dimension (use '*' to use all of them).
                - The order of dropdowns in the MDIM page will follow the order of the dictionary.
                - If any dimension is missing from the dictionary keys, this function will raise an error.
                - The order of dimension values in the MDIM page dropdowns will follow that from each dictionary value (unless '*' is uses, which will be arbitrary).
            - See examples below for more details.
    common_view_config : Dict[str, Any] | None
        Additional config fields to add to each view, e.g.
        {"chartTypes": ["LineChart"], "hasMapTab": True, "tab": "map"}
    indicators_slug: str
        Name of the slug for the indicator. Default is 'indicator'.

    EXAMPLES
    --------

    EXAMPLE 1: There are various indicators with dimensions, we want to expand all their dimensions and their values

    ```python
    config = expand_config(tb=tb)
    ```

    EXAMPLE 2: There are multiple indicators, but we focus on 'deaths'. There are dimensions 'sex', 'age' and 'cause' and we want to expand them all completely, in this order.

    ```python
    config = expand_config(
        tb=tb,
        indicator_name=["deaths"],
        dimensions=[
            "sex",
            "age",
            "cause",
        ]
    )

    EXAMPLE 3: Same as Example 2, but (i) we also consider indicator 'cases', and (ii) for 'cause' we only want to use values 'aids' and 'cancer', in this order.

    ```python
    config = expand_config(
        tb=tb,
        indicator_name=["deaths", "cases"],
        dimensions={
            "sex": "*",
            "age": "*",
            "cause": ["aids", "cancer"],
        }
    )
    """

    # Partial configuration
    config_partial = {}

    # Initiate expander object
    expander = MDIMConfigExpander(
        tb=tb,
        indicators_slug=indicators_slug,
        indicator_names=indicator_names,
    )

    # Combine indicator information with dimensions (only when multiple indicators are given)
    if len(expander.indicator_names) > 1:
        if dimensions is None:
            dimensions = {dim: "*" for dim in expander.dimension_names}
        elif isinstance(dimensions, list):
            dimensions = {dim: "*" for dim in dimensions}
        dimensions = {
            indicators_slug: expander.indicator_names,
            **{k: v for k, v in dimensions.items() if k != indicators_slug},
        }

    # EXPAND CHART_DIMENSIONS
    config_partial["dimensions"] = expander.build_dimensions(
        dimensions=dimensions,
    )

    # EXPAND VIEWS
    config_partial["views"] = expander.build_views(
        common_view_config=common_view_config,
    )

    return config_partial


def upsert_multidim_data_page(
    config: dict,
    paths: PathFinder,
    mdim_name: Optional[str] = None,
    tolerate_extra_indicators: bool = False,
    owid_env: Optional[OWIDEnv] = None,
) -> None:
    """Import MDIM config to DB.

    Args:
    -----

    slug: str
        Slug of the MDIM page. MDIM will be published at /slug
    config: dict
        MDIM configuration.
    paths: PathFinder
        Pass `paths = PathFinder(__file__)` from the script where this function is called.
    mdim_name: str
        Name of the MDIM page. Default is short_name from mdim catalog path.
    owid_env: Optional[OWIDEnv]
        Environment where to publish the MDIM page.
    """
    dependencies = paths.dependencies
    mdim_catalog_path = f"{paths.namespace}/{paths.version}/{paths.short_name}#{mdim_name or paths.short_name}"

    # Read config as structured object
    mdim = Multidim.from_dict(config)

    # Edit views
    process_views(mdim, dependencies=dependencies)

    # TODO: Possibly add other edits (to dimensions?)

    # Upsert to DB
    _upsert_multidim_data_page(mdim_catalog_path, mdim, tolerate_extra_indicators, owid_env)


def process_views(mdim: Multidim, dependencies: Set[str]):
    """Process views in MDIM configuration.

    This includes:
        - Make sure that catalog paths for indicators are complete.
        - TODO: Process views with multiple indicators to have adequate metadata
    """
    # Get table information by table name, and table URI
    tables_by_name = get_tables_by_name_mapping(dependencies)
    # tables_by_uri = get_tables_by_uri_mapping(tables_by_name)  # This is to be used when processing views with multiple indicators

    # Go through all views and expand catalog paths
    for view in mdim.views:
        # Update indicators for each dimension, making sure they have the complete URI
        view.expand_paths(tables_by_name)

        # Combine metadata/config with definitions.common_views
        if (mdim.definitions is not None) and (mdim.definitions.common_views is not None):
            view.combine_with_common(mdim.definitions.common_views)

        # Combine metadata in views which contain multiple indicators
        if view.metadata_is_needed:  # Check if view "contains multiple indicators"
            # TODO
            # view["metadata"] = build_view_metadata_multi(indicators, tables_by_uri)
            log.info(
                f"View with multiple indicators detected. You should edit its `metadata` field to reflect that! This will be done programmatically in the future. Check view with dimensions {view.dimensions}"
            )


def _upsert_multidim_data_page(
    mdim_catalog_path: str, mdim: Multidim, tolerate_extra_indicators: bool, owid_env: Optional[OWIDEnv] = None
) -> None:
    """Actual upsert to DB."""
    # Ensure we have an environment set
    if owid_env is None:
        owid_env = OWID_ENV

    # Validate config
    mdim.validate_schema(SCHEMAS_DIR / "multidim-schema.json")
    validate_collection_config(mdim, owid_env.engine, tolerate_extra_indicators)

    # Replace especial fields URIs with IDs (e.g. sortColumnSlug).
    # TODO: I think we could move this to the Grapher side.
    config = replace_catalog_paths_with_ids(mdim.to_dict())

    # Upsert config via Admin API
    admin_api = AdminAPI(owid_env)
    admin_api.put_mdim_config(mdim_catalog_path, config)


def build_view_metadata_multi(indicators: List[Dict[str, str]], tables_by_uri: Dict[str, Table]):
    """TODO: Combine the metadata from the indicators in the view.

    Ideas:
    -----
    `indicators` contains the URIs of all indicators used in the view. `tables_by_uri` contains the table objects (and therefore their metadata) easily accessible by their URI. With this information, we can get the metadata of each indicator as:

    ```
    for indicator in indicators:
        # Get indicator metadata
        table_uri, indicator_name = indicator["path"].split("#)
        metadata = tables_by_uri[table_uri][indicator_name].metadata

        # We also have access on how this indicator was in use (was it for dimension 'y', or 'x'?)
        dimension = indicator["dimension"] # This can be 'y', 'x', 'size', 'color', etc.
    ```

    Arguments:
    ----------

    indicators : List[Dict[str, str]]
        List of indicators in the view. Each element comes as a record {"path": "...", "dimension": "..."}. The path is the complete URI of the indicator.
    tables_by_uri : Dict[str, Table]
        Mapping of table URIs to table objects.
    """
    pass


def get_tables_by_uri_mapping(tables_by_name: Dict[str, List[Table]]) -> Dict[str, Table]:
    """Mapping table URIs (complete) to table objects."""
    mapping = {}
    for table_name, tables in tables_by_name.items():
        for table in tables:
            uri = table.dataset.uri + "/" + table_name
            mapping[uri] = table
    return mapping


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


####################################################################################################
# Config auto-expander: Expand configuration from a table. This config is partial!
####################################################################################################
class MDIMConfigExpander:
    def __init__(self, tb: Table, indicators_slug: str, indicator_names: Optional[Union[str, List[str]]] = None):
        self.indicators_slug = indicators_slug
        self.build_df_dims(tb, indicator_names)
        self.short_name = tb.m.short_name
        # Get table dimensions from metadata if available, exclude country, year, and date
        self.tb_dims = [d for d in (tb.m.dimensions or []) if d["slug"] not in ("country", "year", "date")]

    @property
    def dimension_names(self):
        return [col for col in self.df_dims.columns if col not in ["short_name"]]

    def build_dimensions(
        self,
        dimensions: Optional[Union[List[str], Dict[str, Union[List[str], str]]]] = None,
    ):
        """Create the specs for each dimension."""
        # Support dimension is None
        if dimensions is None:
            # If table defines dimensions, use them
            if self.tb_dims:
                dimensions = [d["slug"] for d in self.tb_dims]
            else:
                # If dimensions is None, use a list with all dimension names (in no particular order)
                dimensions = [col for col in self.df_dims.columns if col not in ["short_name"]]
        else:
            # log.warning("It's recommended to set dimensions in Table metadata.")
            pass

        # Support dimensions if it is a list/dict
        config_dimensions = []
        if isinstance(dimensions, (list, dict)):
            # Sanity check: All dimension names should be present in the list or dictionary
            _check_intersection_iters(
                self.dimension_names,
                dimensions,
                key_name="dimensions",
            )

            # Add dimension entry and add it to dimensions
            for dim in dimensions:
                dim_values_available = list(self.df_dims[dim].unique())

                # If list, we don't care about dimension value order
                if isinstance(dimensions, list):
                    dim_values = dim_values_available
                # If dictionary, let's use the order (unless '*' is used)!
                else:
                    dim_values = dimensions[dim]
                    if dim_values == "*":
                        dim_values = dim_values_available
                    elif not isinstance(dim_values, list):
                        # Sanity check: besides exceptions above (where we allow dim_values to be a string initially), dim_values should be a list
                        raise ValueError(
                            f"Unexpected value for dimension `{dim}`. Please review `dimensions`: '{dim_values}'!"
                        )

                # Sanity check: values in dim_values are expected
                _check_intersection_iters(
                    dim_values_available,
                    dim_values,
                    key_name=f"dimension={dim}",
                    check_missing=False,
                )

                # Build choices for given dimension
                choices = [
                    {
                        "slug": val,
                        "name": val,
                        "description": None,
                    }
                    for val in dim_values
                ]

                # Build dimension
                if self.tb_dims:
                    # Use full name from table if available
                    dim_name = next(d["name"] for d in self.tb_dims if d["slug"] == dim)
                else:
                    # Otherwise use slug
                    dim_name = dim

                dimension = {
                    "slug": dim,
                    "name": dim_name,
                    "choices": choices,
                }

                # Add dimension to config
                config_dimensions.append(dimension)

        return config_dimensions

    def build_views(self, common_view_config):
        """Generate one view for each indicator in the table."""
        config_views = []
        for _, indicator in self.df_dims.iterrows():
            view = {
                "dimensions": {dim_name: indicator[dim_name] for dim_name in self.dimension_names},
                "indicators": {
                    "y": f"{self.short_name}#{indicator.short_name}",  # TODO: Add support for (i) support "x", "color", "size"; (ii) display settings
                },
            }
            if common_view_config:
                view["config"] = common_view_config
            config_views.append(view)

        return config_views

    def build_df_dims(self, tb: Table, indicator_names: Optional[Union[str, List[str]]]):
        """Build dataframe with dimensional information from table tb.

        It contains the following columns:
            - indicator: Values in this column refer to an 'actual' indicator.
            - dimension1Name: Values in this column provide the dimension value for dimension1Name. E.g. '10-20' in case dimension1Name is 'age'.
            - dimensionXName: Same as dimension1Name. There can be several...
            ...
            - short_name: Name of the column in the original table.

        Example:

        indicator	place	                short_name
        trend	    Grocery and pharmacy	trend__place_grocery_and_pharmacy
        trend	    Parks	                trend__place_parks
        trend	    Residential	            trend__place_residential
        trend	    Retail and recreation	trend__place_retail_and_recreation
        trend	    Transit stations	    trend__place_transit_stations
        trend	    Workplaces	            trend__place_workplaces
        """
        df_dims = self._build_df_dims(tb)

        # Ensure that indicator_name is a list, if any value is given
        if isinstance(indicator_names, str):
            indicator_names = [indicator_names]

        # SANITY CHECKS
        self.indicator_names = self._sanity_checks_df_dims(indicator_names, df_dims)

        # Keep dimensions only for relevant indicators
        self.df_dims = df_dims.loc[df_dims[self.indicators_slug].isin(self.indicator_names)]

        # Drop indicator column if indicator_names is of length 1
        if len(self.indicator_names) == 1:
            self.df_dims = self.df_dims.drop(columns=["indicator"])

        # Final checks
        assert all(
            isinstance(indicator_name, str) for indicator_name in self.indicator_names
        ), "Class attribute indicator_names should be a list of string!"
        assert not self.df_dims.empty, "df_dims can't be empty!"

    def _build_df_dims(self, tb):
        """Build dataframe with dimensional information from table tb."""
        records = []
        for col in tb.columns:
            dims = tb[col].m.dimensions
            if dims:
                assert tb[col].m.original_short_name, "Missing metadata.original_short_name for dimensions!"
                row = {
                    self.indicators_slug: tb[col].m.original_short_name,
                    "short_name": col,
                }
                # Add dimensional info
                for name in dims.keys():
                    if name in {self.indicators_slug, "short_name"}:
                        raise ValueError(f"Dimension name `{name}` is reserved. Please use another one!")

                row = {**row, **dims}

                # Add entry to records
                records.append(row)

        # Build dataframe with dimensional information
        df_dims = pd.DataFrame(records)

        # Re-order columns
        cols_dims = [col for col in df_dims.columns if col not in [self.indicators_slug, "short_name"]]
        df_dims = df_dims[[self.indicators_slug] + sorted(cols_dims) + ["short_name"]]
        return df_dims

    def _sanity_checks_df_dims(self, indicator_names: Optional[List[str]], df_dims: pd.DataFrame):
        """Sanity checks of df_dims."""
        # List with names of indicators and dimensions
        indicator_names_available = list(df_dims[self.indicators_slug].unique())

        # If no indicator name is provided, there should only be one in the table!
        if indicator_names is None:
            if len(indicator_names_available) != 1:
                raise ValueError(
                    f"There are multiple indicators {indicator_names}, but no `indicator_name` was provided. Please specify at least one!"
                )
            # If only one indicator available, set it as the indicator name
            return indicator_names_available
        # Check that given indicator_names are available (i.e. are present in indicator_names_available)
        indicator_names_unknown = set(indicator_names).difference(set(indicator_names_available))
        if indicator_names_unknown:
            raise ValueError(
                f"Indicators `{', '.join(indicator_names_unknown)}` not found in the table. Available are: {', '.join(indicator_names_available)}"
            )

        return indicator_names


def combine_config_dimensions(
    config_dimensions: List[Dict[str, Any]],
    config_dimensions_yaml: List[Dict[str, Any]],
    choices_top: bool = False,
    dimensions_top: bool = False,
):
    """Combine the dimension configuration from the YAML file with the one generated programmatically.

    There are various strategies that we could follow here, but currently:

    - We consider the union of config_dimensions (returned by expander.build_dimensions) nad config_dimensions_yaml.
    - These are kept as-is, unless they are in the YML config, in which case they are overwritten.

    Other possible strategies:

    - We could do the reverse, and only consider the fields from config_dimensions_yaml. I'm personally unsure when this could be valuable.


    Arguments
    ---------
    config_dimensions: List[Dict[str, Any]]
        Generated by expander.build_dimensions.
    config_dimensions_yaml:  List[Dict[str, Any]]
        From the YAML file.
    choices_top: bool
        Set to True to place the choices from `config_dimensions` first.
    dimensions_top: bool
        Set to True to place the dimensions from `config_dimensions` first.

    TODO:

        - I think we need to add more checks to ensure that there is nothing weird being produced here.
    """

    config_dimensions_combined = deepcopy(config_dimensions)
    dims_overwrite = records_to_dictionary(config_dimensions_yaml, "slug")

    # Overwrite dimensions
    for dim in config_dimensions_combined:
        slug_dim = dim["slug"]
        if slug_dim in dims_overwrite:
            # Get dimension data to overwrite, remove it from dictionary
            dim_overwrite = dims_overwrite.pop(slug_dim)

            # Overwrite dimension name
            dim["name"] = dim_overwrite.get("name", dim["name"])

            # Overwrite choices
            if "choices" in dim_overwrite:
                choices_overwrite = records_to_dictionary(
                    dim_overwrite["choices"],
                    "slug",
                )
                assert (
                    "choices" in dim
                ), f"Choices not found in dimension: {dim}! This is rare, please report this issue!"
                for choice in dim["choices"]:
                    slug_choice = choice["slug"]
                    if slug_choice in choices_overwrite:
                        # Get dimension data to overwrite, remove it from dictionary
                        choice_overwrite = choices_overwrite.pop(slug_choice)

                        # Overwrite choice name
                        choice["name"] = choice_overwrite.get("name", dim["name"])
                        # Overwrite choice description
                        choice["description"] = choice_overwrite.get("description", choice["description"])

                # Handle choices from YAML not present in config_dimensions
                if choices_overwrite:
                    missing_choices = []
                    for slug, values in choices_overwrite.items():
                        choice = {"slug": slug, **values}
                        missing_choices.append(choice)

                    if choices_top:
                        dim["choices"] += missing_choices
                    else:
                        dim["choices"] = missing_choices + dim["choices"]

    # Handle dimensions from YAML not present in config_dimensions
    if dims_overwrite:
        missing_dims = []
        for slug, values in dims_overwrite.items():
            dim = {"slug": slug, **values}
            missing_dims.append(dim)

        if dimensions_top:
            config_dimensions_combined += missing_dims
        else:
            config_dimensions_combined = missing_dims + config_dimensions_combined

    return config_dimensions_combined


def _check_intersection_iters(
    items_expected,
    items_given,
    key_name: Optional[str] = None,
    check_dups: bool = True,
    check_missing: bool = True,
    check_unexpected: bool = True,
):
    """Check that the intersection/overlap of items_expected and items_given is as expected.

    It checks that:
        - There are no duplicate items in items_given. Unless check_dups is False.
        - items_given cover all the expected items. Unless check_missing is False.
        - items_given don't cover unexpected items. Unless check_unexpected is False.
    """
    if key_name is None:
        key_name = "items_given"

    # Sanity check 1: No duplicate items
    if check_dups and (len(items_given) != len(set(items_given))):
        raise ValueError(f"Duplicate items are not allowed. Please review `{key_name}`!")

    # Sanity check 2: Items should cover all the expected items (there is none missing!)
    items_missing = set(items_expected) - set(items_given)
    if check_missing and items_missing:
        raise ValueError(f"Missing items: {', '.join([f'`{d}`' for d in items_missing])} Please review `{key_name}`!")

    # Sanity check 3: Items shouldn't cover unexpected items (more than needed!)
    items_unexpected = set(items_given) - set(items_expected)
    if check_unexpected and items_unexpected:
        raise ValueError(
            f"Unexpected items: {', '.join([f'`{d}`' for d in items_unexpected])}. Please review `{key_name}`!"
        )


def group_views(views: list[dict[str, Any]], by: list[str]) -> list[dict[str, Any]]:
    """
    Group views by the specified dimensions. Concatenate indicators for the same group.

    :param views: List of views dictionaries.
    :param by: List of dimensions to group by.
    """
    views = deepcopy(views)

    grouped_views = {}
    for view in views:
        # Group key
        key = tuple(view["dimensions"][dim] for dim in by)

        if key not in grouped_views:
            # Ensure 'y' is a single indicator before turning it into a list
            assert not isinstance(view["indicators"]["y"], list), "Expected 'y' to be a single indicator, not a list"

            if set(view["indicators"].keys()) != {"y"}:
                raise NotImplementedError(
                    "Only 'y' indicator is supported in groupby. Adapt the code for other fields."
                )

            view["indicators"]["y"] = [view["indicators"]["y"]]

            # Add to dictionary
            grouped_views[key] = view
        else:
            grouped_views[key]["indicators"]["y"].append(view["indicators"]["y"])

    return list(grouped_views.values())

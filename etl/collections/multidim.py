"""TODO:

- Structure MDIM config and related objects in a more pythonic way (e.g. dataclass).
- Need to be able to quickly validate the configs against schemas.
- We should try to keep explorers in mind, and see this tooling as something we may want to use for them, too.
"""

import json
import re
from collections import defaultdict
from copy import deepcopy
from itertools import product
from typing import Any, Dict, List, Optional, Set, Union

import fastjsonschema
import pandas as pd
import yaml
from deprecated import deprecated
from owid.catalog import Dataset, Table
from sqlalchemy.engine import Engine
from structlog import get_logger

from apps.chart_sync.admin_api import AdminAPI
from etl.collections.utils import records_to_dictionary
from etl.config import OWID_ENV, OWIDEnv
from etl.db import read_sql
from etl.grapher.io import trim_long_variable_name
from etl.helpers import map_indicator_path_to_id
from etl.paths import DATA_DIR, SCHEMAS_DIR

# Initialize logger.
log = get_logger()
# Dimensions: These are the expected possible dimensions
DIMENSIONS = ["y", "x", "size", "color"]


def expand_config(
    tb: Table,
    indicator_name: Optional[str] = None,
    dimensions: Optional[Union[List[str], Dict[str, Union[List[str], str]]]] = None,
    common_view_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Create partial config (dimensions and views) from multi-dimensional indicator in table `tb`.

    This method returns the configuration generated from the table `tb`. It assumes that it only has one indicator, unless `indicator_name` is provided. It will expand all dimensions and their values, unless `dimensions` is provided.

    To tweak which dimensions or dimension values are expanded use argument `dimensions`.

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
    indicator_name : str | None
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
                - Keys represent the dimensions, and values are the set of values to consider for each dimension (use '*' to use all of them).
                - The order of dropdowns in the MDIM page will follow the order of the dictionary.
                - If any dimension is missing from the dictionary keys, this function will raise an error.
                - The order of dimension values in the MDIM page dropdowns will follow that from each dictionary value (unless '*' is uses, which will be arbitrary).
            - See examples below for more details.
    common_view_config : Dict[str, Any] | None
        Additional config fields to add to each view, e.g.
        {"chartTypes": ["LineChart"], "hasMapTab": True, "tab": "map"}


    EXAMPLES
    --------

    EXAMPLE 1: There is only one indicator, we want to expand all dimensions and their values

    ```python
    config = expand_config(tb=tb)
    ```

    EXAMPLE 2: There are multiple indicators, but we focus on 'deaths'. There are dimensions 'sex', 'age' and 'cause' and we want to expand them all completely, in this order.

    ```python
    config = expand_config(
        tb=tb,
        indicator_name="deaths",
        dimensions=[
            "sex",
            "age",
            "cause",
        ]
    )

    EXAMPLE 3: Same as Example 2, but for 'cause' we only want to use values 'aids' and 'cancer', in this order.

    ```python
    config = expand_config(
        tb=tb,
        indicator_name="deaths",
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
    expander = MDIMConfigExpander(tb, indicator_name)

    # EXPAND DIMENSIONS
    config_partial["dimensions"] = expander.build_dimensions(
        dimensions=dimensions,
    )

    # EXPAND VIEWS
    config_partial["views"] = expander.build_views(
        common_view_config=common_view_config,
    )

    return config_partial


def upsert_multidim_data_page(
    slug: str, config: dict, dependencies: Set[str] = set(), owid_env: Optional[OWIDEnv] = None
) -> None:
    """Import MDIM config to DB.

    Args:
    -----

    slug: str
        Slug of the MDIM page. MDIM will be published at /slug
    config: dict
        MDIM configuration.
    owid_env: Optional[OWIDEnv]
        Environment where to publish the MDIM page.
    """
    # Edit views
    process_mdim_views(config, dependencies=dependencies)

    # TODO: Possibly add other edits (to dimensions?)

    # Upser to DB
    _upsert_multidim_data_page(slug, config, owid_env)


def process_mdim_views(config: dict, dependencies: Set[str]):
    """Process views in MDIM configuration.

    This includes:
        - Make sure that catalog paths for indicators are complete.
        - TODO: Process views with multiple indicators to have adequate metadata
    """
    # Get table information by table name, and table URI
    tables_by_name = get_tables_by_name_mapping(dependencies)
    # tables_by_uri = get_tables_by_uri_mapping(tables_by_name)  # This is to be used when processing views with multiple indicators

    # Go through all views and expand catalog paths
    for view in config["views"]:
        # Update indicators for each dimension, making sure they have the complete URI
        expand_catalog_paths(view, tables_by_name=tables_by_name)

        # Combine metadata in views which contain multiple indicators
        indicators = get_indicators_in_view(view)
        if (len(indicators) > 1) and ("metadata" not in view):  # Check if view "contains multiple indicators"
            # TODO
            # view["metadata"] = build_view_metadata_multi(indicators, tables_by_uri)
            log.info(
                f"View with multiple indicators detected. You should edit its `metadata` field to reflect that! This will be done programmatically in the future. Check view with dimensions {view['dimensions']}"
            )
            pass


def _upsert_multidim_data_page(slug: str, config: dict, owid_env: Optional[OWIDEnv] = None) -> None:
    """Actual upsert to DB."""
    # Ensure we have an environment set
    if owid_env is None:
        owid_env = OWID_ENV

    # Validate config
    validate_schema(config)
    validate_multidim_config(config, owid_env.engine)

    # Replace especial fields URIs with IDs (e.g. sortColumnSlug).
    # TODO: I think we could move this to the Grapher side.
    config = replace_catalog_paths_with_ids(config)

    # Upsert config via Admin API
    admin_api = AdminAPI(owid_env)
    admin_api.put_mdim_config(slug, config)


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


def get_tables_by_name_mapping(dependencies: Set[str]) -> Dict[str, List[Table]]:
    """Dictionary mapping table short name to table object.

    Note that the format is {"table_name": [tb], ...}. This is because there could be collisions where multiple table names are mapped to the same table (e.g. two datasets could have a table with the same name).
    """
    tb_name_to_tb = defaultdict(list)

    for dep in dependencies:
        ## Ignore non-grapher dependencies
        if not re.match(r"^(data|data-private)://grapher/", dep):
            continue

        uri = re.sub(r"^(data|data-private)://", "", dep)
        ds = Dataset(DATA_DIR / uri)
        for table_name in ds.table_names:
            tb_name_to_tb[table_name].append(ds.read(table_name, load_data=False))

    return tb_name_to_tb


def expand_catalog_paths(view: Dict[Any, Any], tables_by_name: Dict[str, List[Table]]) -> Dict[Any, Any]:
    """Expand catalog paths in views to full dataset URIs.

    This function updates the given configuration dictionary in-place by modifying the dimension ('y', 'x', 'size', 'color') entries under "indicators" in each view. If an entry does not contain a '/',
    it is assumed to be a table name that must be expanded to a full dataset URI based on
    the provided dependencies.

    NOTE: Possible improvements for internal function `_expand`:
        - we should make this function a bit more robust when checking the URIs.
        - currently we only allow for 'table#indicator' format. We should also allow for other cases that could be useful in the event of name collisions, e.g. 'dataset/indicator#subindicator'.

    Args:
        config (dict): Configuration dictionary containing views.
        tables_by_name (Dict[str, List[Table]]): Mapping of table short names to tables.
    """

    def _expand_catalog_path(indicator: Union[str, Dict[str, str]]) -> Union[str, Dict[str, str]]:
        """Return same indicator, but with complete catalog path."""

        def _expand(indicator: str):
            assert "#" in indicator, f"Missing '#' in indicator! '{indicator}'"

            # Complete dataset URI
            if "/" in indicator:
                return indicator
            # table#indicator format
            else:
                indicator_split = indicator.split("#")

                # Check format is actually table#indicator
                assert (len(indicator_split) == 2) & (
                    indicator_split[0] != ""
                ), f"Expected 'table#indicator' format. Instead found {indicator}"

                # Check table is in any of the datasets!
                assert (
                    indicator_split[0] in tables_by_name
                ), f"Table name `{indicator_split[0]}` not found in dependency tables! Available tables are: {', '.join(tables_by_name.keys())}"

                # Check table name to table mapping is unique
                assert (
                    len(tables_by_name[indicator_split[0]]) == 1
                ), f"There are multiple dependencies (datasets) with a table named {indicator_split[0]}. Please use the complete dataset URI in this case."

                # Check dataset in table metadata is not None
                tb = tables_by_name[indicator_split[0]][0]
                assert tb.m.dataset is not None, f"Dataset not found for table {indicator_split[0]}"

                # Build URI
                return tb.m.dataset.uri + "/" + indicator

        # Expand catalog path if it's a string
        if isinstance(indicator, str):
            return _expand(indicator)
        # Expand catalog path if it's a dictionary
        elif isinstance(indicator, dict):
            assert "catalogPath" in indicator, "Expected 'catalogPath' key in indicator dictionary"
            indicator["catalogPath"] = _expand(indicator["catalogPath"])
            return indicator

    # Update indicators for each dimension
    for dim in DIMENSIONS:
        if dim in view["indicators"]:
            if isinstance(view["indicators"][dim], list):
                view["indicators"][dim] = [_expand_catalog_path(dim) for dim in view["indicators"][dim]]
            else:
                view["indicators"][dim] = _expand_catalog_path(view["indicators"][dim])

    # Update indicators from sortColumnSlug
    if "config" in view:
        if "sortColumnSlug" in view["config"]:
            view["config"]["sortColumnSlug"] = _expand_catalog_path(view["config"]["sortColumnSlug"])

    # Update indicators from map.columnSlug
    if "config" in view:
        if "map" in view["config"]:
            if "columnSlug" in view["config"]["map"]:
                view["config"]["map"]["columnSlug"] = _expand_catalog_path(view["config"]["map"]["columnSlug"])

    return view


def _extract_catalog_path(indicator_raw):
    "Indicator spec can come either as a plain string, or a dictionary."
    if isinstance(indicator_raw, str):
        return indicator_raw
    elif isinstance(indicator_raw, dict):
        assert "catalogPath" in indicator_raw
        return indicator_raw["catalogPath"]
    else:
        raise ValueError(f"Unexpected indicator property type: {indicator_raw}")


def get_indicators_in_view(view):
    """Get the list of indicators in use in a view.

    It returns the list as a list of records:

    [
        {
            "path": "data://path/to/dataset#indicator",
            "dimension": "y"
        },
        ...
    ]

    TODO: This is being called twice, maybe there is a way to just call it once. Maybe if it is an attribute of a class?
    """
    indicators_view = []
    # Get indicators from dimensions
    for dim in DIMENSIONS:
        if dim in view["indicators"]:
            indicator_raw = view["indicators"][dim]
            if isinstance(indicator_raw, list):
                assert dim == "y", "Only `y` can come as a list"
                indicators_view += [
                    {
                        "path": _extract_catalog_path(ind),
                        "dimension": dim,
                    }
                    for ind in indicator_raw
                ]
            else:
                indicators_view.append(
                    {
                        "path": _extract_catalog_path(indicator_raw),
                        "dimension": dim,
                    }
                )
    return indicators_view


def validate_schema(config: dict) -> None:
    schema_path = SCHEMAS_DIR / "multidim-schema.json"
    with open(schema_path) as f:
        schema = json.load(f)

    validator = fastjsonschema.compile(schema)

    try:
        validator(config)  # type: ignore
    except fastjsonschema.JsonSchemaException as e:
        raise ValueError(f"Config validation error: {e.message}")  # type: ignore


def validate_multidim_config(config: dict, engine: Engine) -> None:
    # Ensure that all views are in choices
    for dim in config["dimensions"]:
        allowed_slugs = [choice["slug"] for choice in dim["choices"]]

        for view in config["views"]:
            for dim_name, dim_value in view["dimensions"].items():
                if dim_name == dim["slug"] and dim_value not in allowed_slugs:
                    raise ValueError(
                        f"Slug `{dim_value}` does not exist in dimension `{dim_name}`. View:\n\n{yaml.dump(view)}"
                    )

    # Get all used indicators
    indicators = []
    for view in config["views"]:
        # Get indicators from dimensions
        indicators_view = get_indicators_in_view(view)
        indicators_view = [ind["path"] for ind in indicators_view]
        indicators_extra = []

        # Get indicators from sortColumnSlug
        if "config" in view:
            if "sortColumnSlug" in view["config"]:
                indicators_extra.append(_extract_catalog_path(view["config"]["sortColumnSlug"]))

        # Update indicators from map.columnSlug
        if "config" in view:
            if "map" in view["config"]:
                if "columnSlug" in view["config"]["map"]:
                    indicators_extra.append(_extract_catalog_path(view["config"]["map"]["columnSlug"]))

        # All indicators in `indicators_extra` should be in `indicators`! E.g. you can't sort by an indicator that is not in the chart!
        ## E.g. the indicator used to sort, should be in use in the chart! Or, the indicator in the map tab should be in use in the chart!
        invalid_indicators = set(indicators_extra).difference(set(indicators_view))
        if invalid_indicators:
            raise ValueError(
                f"Extra indicators not in use. This means that some indicators are referenced in the chart config (e.g. map.columnSlug or sortColumnSlug), but never used in the chart tab. Unexpected indicators: {invalid_indicators}"
            )

        indicators.extend(indicators_view)

    # Make sure indicators are unique
    indicators = list(set(indicators))

    # Validate duplicate views
    seen_dims = set()
    for view in config["views"]:
        dims = tuple(view["dimensions"].items())
        if dims in seen_dims:
            raise ValueError(f"Duplicate view:\n\n{yaml.dump(view['dimensions'])}")
        seen_dims.add(dims)

    # NOTE: this is allowed, some views might contain other views
    # Check uniqueness
    # inds = pd.Series(indicators)
    # vc = inds.value_counts()
    # if vc[vc > 1].any():
    #     raise ValueError(f"Duplicate indicators: {vc[vc > 1].index.tolist()}")

    # Check that all indicators exist
    q = """
    select
        id,
        catalogPath
    from variables
    where catalogPath in %(indicators)s
    """
    df = read_sql(q, engine, params={"indicators": tuple(indicators)})
    missing_indicators = set(indicators) - set(df["catalogPath"])
    if missing_indicators:
        raise ValueError(f"Missing indicators in DB: {missing_indicators}")


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
    def __init__(self, tb: Table, indicator_name: Optional[str] = None):
        self.build_df_dims(tb, indicator_name)
        self.short_name = tb.m.short_name

    @property
    def dimension_names(self):
        return [col for col in self.df_dims.columns if col not in ["indicator", "short_name"]]

    def build_dimensions(
        self,
        dimensions: Optional[Union[List[str], Dict[str, Union[List[str], str]]]] = None,
    ):
        """Create the specs for each dimension."""
        # Support dimension is None
        ## If dimensions is None, use a list with all dimension names (in no particular order)
        if dimensions is None:
            dimensions = [col for col in self.df_dims.columns if col not in ["short_name"]]

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
                dimension = {
                    "slug": dim,
                    "name": dim,
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

    def build_df_dims(self, tb, indicator_name):
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

        # SANITY CHECKS
        self.indicator_name = self._sanity_checks_df_dims(indicator_name, df_dims)

        # Keep dimensions only for relevant indicator
        self.df_dims = df_dims.loc[df_dims["indicator"] == self.indicator_name].drop(columns=["indicator"])

        # Final checks
        assert isinstance(self.indicator_name, str), "Indicator name should be a string!"
        assert not self.df_dims.empty, "df_dims can't be empty!"

    def _build_df_dims(self, tb):
        """Build dataframe with dimensional information from table tb."""
        records = []
        for col in tb.columns:
            if tb[col].metadata.additional_info and ("dimensions" in tb[col].metadata.additional_info):
                dims = tb[col].metadata.additional_info["dimensions"]

                assert "originalShortName" in dims, "Missing indicator name in dimensions metadata!"
                row = {
                    "indicator": dims["originalShortName"],
                    "short_name": col,
                }
                # Add dimensional info
                assert "filters" in dims, "Missing filters in dimensions metadata!"
                filters = dims["filters"]
                for f in filters:
                    row[f["name"]] = f["value"]

                # Add entry to records
                records.append(row)

        # Build dataframe with dimensional information
        df_dims = pd.DataFrame(records)

        # Re-order columns
        cols_dims = [col for col in df_dims.columns if col not in ["indicator", "short_name"]]
        df_dims = df_dims[["indicator"] + sorted(cols_dims) + ["short_name"]]
        return df_dims

    def _sanity_checks_df_dims(self, indicator_name, df_dims):
        """Sanity checks of df_dims."""
        # List with names of indicators and dimensions
        indicator_names = list(df_dims["indicator"].unique())

        # If no indicator name is provided, there should only be one in the table!
        if indicator_name is None:
            if len(indicator_names) != 1:
                raise ValueError("There are multiple indicators, but no `indicator_name` was provided.")
            # If only one indicator available, set it as the indicator name
            indicator_name = indicator_names[0]
        # If indicator name is given, make sure it is present in the table!
        if indicator_name not in indicator_names:
            raise ValueError(
                f"Indicator `{indicator_name}` not found in the table. Available are: {', '.join(indicator_names)}"
            )

        return indicator_name


def combine_config_dimensions(
    config_dimensions,
    config_dimensions_yaml,
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
        config_dimensions: Generated by expander.build_dimensions.
        config_dimensions_yaml: From the YAML file.
        choices_top: Set to True to place the choices from config_dimensions first.
        dimensions_top: Set to True to place the dimensions from config_dimensions first.

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
            config_dimensions_combined = missing_dims + config_dimensions_combined
        else:
            config_dimensions_combined += missing_dims

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


####################################################################################################
# DEPRECATED FUNCTIONS
####################################################################################################
@deprecated("This function relies on specific column naming convention. Use `expand_config` instead.")
def generate_views_for_dimensions(
    dimensions, tables, dimensions_order_in_slug=None, additional_config=None, warn_on_missing_combinations=True
):
    """Generate individual views for all possible combinations of dimensions in a list of flattened tables.

    Parameters
    ----------
    dimensions : List[Dict[str, Any]]
        Dimensions, as given in the configuration of the multidim step, e.g.
        [
            {'slug': 'frequency', 'name': 'Frequency', 'choices': [{'slug': 'annual','name': 'Annual'}, {'slug': 'monthly', 'name': 'Monthly'}]},
            {'slug': 'source', 'name': 'Energy source', 'choices': [{'slug': 'electricity', 'name': 'Electricity'}, {'slug': 'gas', 'name': 'Gas'}]},
            ...
        ]
    tables : List[Table]
        Tables whose indicator views will be generated.
    dimensions_order_in_slug : Tuple[str], optional
        Dimension names, as they appear in "dimensions", and in the order in which they are spelled out in indicator names. For example, if indicator names are, e.g. annual_electricity_euros, then dimensions_order_in_slug would be ("frequency", "source", "unit").
    additional_config : _type_, optional
        Additional config fields to add to each view, e.g.
        {"chartTypes": ["LineChart"], "hasMapTab": True, "tab": "map"}
    warn_on_missing_combinations : bool, optional
        True to warn if any combination of dimensions is not found among the indicators in the given tables.

    Returns
    -------
    results : List[Dict[str, Any]]
        Views configuration, e.g.
        [
            {'dimensions': {'frequency': 'annual', 'source': 'electricity', 'unit': 'euro'}, 'indicators': {'y': 'grapher/energy/2024-11-20/energy_prices/energy_prices_annual#annual_electricity_household_total_price_including_taxes_euro'},
            {'dimensions': {'frequency': 'annual', 'source': 'electricity', 'unit': 'pps'}, 'indicators': {'y': 'grapher/energy/2024-11-20/energy_prices/energy_prices_annual#annual_electricity_household_total_price_including_taxes_pps'},
            ...
        ]

    """
    # Extract all choices for each dimension as (slug, choice_slug) pairs.
    choices = {dim["slug"]: [choice["slug"] for choice in dim["choices"]] for dim in dimensions}
    dimension_slugs_in_config = set(choices.keys())

    # Sanity check for dimensions_order_in_slug.
    if dimensions_order_in_slug:
        dimension_slugs_in_order = set(dimensions_order_in_slug)

        # Check if any slug in the order is missing from the config.
        missing_slugs = dimension_slugs_in_order - dimension_slugs_in_config
        if missing_slugs:
            raise ValueError(
                f"The following dimensions are in 'dimensions_order_in_slug' but not in the config: {missing_slugs}"
            )

        # Check if any slug in the config is missing from the order.
        extra_slugs = dimension_slugs_in_config - dimension_slugs_in_order
        if extra_slugs:
            log.warning(
                f"The following dimensions are in the config but not in 'dimensions_order_in_slug': {extra_slugs}"
            )

        # Reorder choices to match the specified order.
        choices = {dim: choices[dim] for dim in dimensions_order_in_slug if dim in choices}

    # Generate all combinations of the choices.
    all_combinations = list(product(*choices.values()))

    # Create the views.
    results = []
    for combination in all_combinations:
        # Map dimension slugs to the chosen values.
        dimension_mapping = {dim_slug: choice for dim_slug, choice in zip(choices.keys(), combination)}
        slug_combination = "_".join(combination)

        # Find relevant tables for the current combination.
        relevant_table = []
        for table in tables:
            if slug_combination in table:
                relevant_table.append(table)

        # Handle missing or multiple table matches.
        if len(relevant_table) == 0:
            if warn_on_missing_combinations:
                log.warning(f"Combination {slug_combination} not found in tables")
            continue
        elif len(relevant_table) > 1:
            log.warning(f"Combination {slug_combination} found in multiple tables: {relevant_table}")

        # Construct the indicator path.
        indicator_path = f"{relevant_table[0].metadata.dataset.uri}/{relevant_table[0].metadata.short_name}#{trim_long_variable_name(slug_combination)}"
        indicators = {
            "y": indicator_path,
        }
        # Append the combination to results.
        results.append({"dimensions": dimension_mapping, "indicators": indicators})

    if additional_config:
        # Include additional fields in all results.
        for result in results:
            result.update({"config": additional_config})

    return results


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

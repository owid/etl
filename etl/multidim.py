import json
import re
from copy import deepcopy
from itertools import product
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import yaml
from deprecated import deprecated
from owid.catalog import Dataset, Table
from sqlalchemy.engine import Engine
from structlog import get_logger

from apps.chart_sync.admin_api import AdminAPI
from etl.config import OWID_ENV, OWIDEnv
from etl.db import read_sql
from etl.grapher.io import trim_long_variable_name
from etl.helpers import PathFinder, map_indicator_path_to_id
from etl.paths import DATA_DIR

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


def upsert_multidim_data_page(slug: str, config: dict, paths: PathFinder, owid_env: Optional[OWIDEnv] = None) -> None:
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
    adjust_mdim_views(config, dependencies_by_table=paths.dependencies_by_table_name)

    # Upser to DB
    _upsert_multidim_data_page(slug, config, owid_env)


def _upsert_multidim_data_page(slug: str, config: dict, owid_env: Optional[OWIDEnv] = None) -> None:
    # Ensure we have an environment set
    if owid_env is None:
        owid_env = OWID_ENV

    # Validate config
    validate_multidim_config(config, owid_env.engine)

    # Replace especial fields URIs with IDs (e.g. sortColumnSlug).
    # TODO: I think we could move this to the Grapher side.
    config = replace_catalog_paths_with_ids(config)

    # Upsert config via Admin API
    admin_api = AdminAPI(owid_env)
    admin_api.put_mdim_config(slug, config)


def adjust_mdim_views(config: dict, dependencies_by_table: Dict[str, List[Any]]):
    # Go through all views and expand catalog paths
    for view in config["views"]:
        # Update indicators for each dimension, making sure they have the complete URI
        expand_catalog_paths(view, dependencies_by_table=dependencies_by_table)


def expand_catalog_paths(view: Dict[Any, Any], dependencies_by_table: Dict[str, List[Any]]) -> Dict[Any, Any]:
    """Expand catalog paths in views to full dataset URIs.

    This function updates the given configuration dictionary in-place by modifying the dimension ('y', 'x', 'size', 'color') entries under "indicators" in each view. If an entry does not contain a '/',
    it is assumed to be a table name that must be expanded to a full dataset URI based on
    the provided dependencies.

    NOTE: Possible improvements for internal function `_expand`:
        - we should make this function a bit more robust when checking the URIs.
        - currently we only allow for 'table#indicator' format. We should also allow for other cases that could be useful in the event of name collisions, e.g. 'dataset/indicator#subindicator'.

    Args:
        config (dict): Configuration dictionary containing views.
        dependencies (list[str]): List of dependency URIs in the form "data://<path>".
    """
    table_to_dataset_uri = {}
    for k, v in dependencies_by_table.items():
        if len(v) == 1:
            table_to_dataset_uri[k] = v[0]["dataset_uri"]
        else:
            table_to_dataset_uri[k] = None

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
                    indicator_split[0] in table_to_dataset_uri
                ), f"Table name `{indicator_split[0]}` not found in dependency tables! Available tables are: {', '.join(table_to_dataset_uri.keys())}"

                assert (
                    table_to_dataset_uri[indicator_split[0]] is not None
                ), f"There are multiple dependencies (datasets) with a table named {indicator_split[0]}. Please use the complete dataset URI in this case."
                return table_to_dataset_uri[indicator_split[0]] + "/" + indicator

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
        indicators_view = []
        indicators_extra = []
        # Get indicators from dimensions
        for prop in DIMENSIONS:
            if prop in view["indicators"]:
                indicator_raw = view["indicators"][prop]
                if isinstance(indicator_raw, list):
                    assert prop == "y", "Only `y` can come as a list"
                    indicators_view += [_extract_catalog_path(ind) for ind in indicator_raw]
                else:
                    indicators_view.append(_extract_catalog_path(indicator_raw))

        # Get indicators from sortColumnSlug
        if "config" in view:
            if "sortColumnSlug" in view["config"]:
                indicators_extra.append(_extract_catalog_path(view["config"]["sortColumnSlug"]))

        # Update indicators from map.columnSlug
        if "config" in view:
            if "map" in view["config"]:
                if "columnSlug" in view["config"]["map"]:
                    indicators_extra.append(_extract_catalog_path(view["config"]["map"]["columnSlug"]))

        # All indicators in indicators_extra should be in indicators!
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
        self.df_dims = df_dims.loc[df_dims["indicator"] == indicator_name].drop(columns=["indicator"])

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

    def _build_reference_dix(records, key: str):
        """Transform: [{key: ..., a: ..., b: ...}, ...] -> {key: {a: ..., b: ...}, ...}."""

        dix = {}
        for record in records:
            assert key in record, f"`{key}` not found in record: {record}!"
            dix[record[key]] = {k: v for k, v in record.items() if k != key}

        return dix

    config_dimensions_combined = deepcopy(config_dimensions)
    dims_overwrite = _build_reference_dix(config_dimensions_yaml, "slug")

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
                choices_overwrite = _build_reference_dix(
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
@deprecated(
    "This function relies on DB-access. Instead we should rely only on ETL local files. Use `expand_config` instead."
)
def expand_views_with_access_db(
    config: dict, combinations: dict[str, str], table: str, engine: Optional[Engine] = None
) -> list[dict]:
    """Use dimensions from multidim config file and create views from all possible
    combinations of dimensions. Grapher table must use the same dimensions as the
    multidim config file. If the dimension is missing from groupby, it will be set to "all".

    :params config: multidim config file
    :params combinations: dictionary with dimension names as keys and values as dimension values, use * for all values
        e.g. {"metric": "*", "age": "*", "cause": "All causes"}
    :params table: catalog path of the grapher table
    :params engine: SQLAlchemy engine
    """
    if engine is None:
        engine = OWID_ENV.engine

    # Get all allowed values from choices
    choices = {}
    for choice_dict in config["dimensions"]:
        allowed_values = []
        for choice in choice_dict["choices"]:
            allowed_values.append(choice["name"])
        choices[choice_dict["slug"]] = allowed_values

    df = fetch_variables_from_table(table, engine)

    # Filter by allowed values
    for dim_name, allowed_values in choices.items():
        df = df[df[dim_name].isin(allowed_values)]

    groupby = [k for k, v in combinations.items() if v == "*"]

    views = []
    for key, group in df.groupby(groupby):
        dims = {dim_name: dim_value for dim_name, dim_value in zip(groupby, key)}

        # Add values that are not * to dimensions
        for k, v in combinations.items():
            if v != "*":
                dims[k] = v

        # Create view with catalog paths
        catalog_paths = group["catalogPath"].tolist()
        views.append(
            {
                "dimensions": dims,
                "indicators": {
                    "y": catalog_paths if len(catalog_paths) > 1 else catalog_paths[0],
                },
            }
        )

    return views


@deprecated(
    "This function relies on DB-access. Instead we should rely only on ETL local files. Use `expand_config` instead."
)
def fetch_variables_from_table(table: str, engine: Engine) -> pd.DataFrame:
    # fetch variables from MySQL
    q = f"""
    select
        id,
        catalogPath,
        dimensions
    from variables
    where catalogPath like '{table}%%'
    """
    df = read_sql(q, engine)

    if df.empty:
        raise ValueError(f"No data found for table {table}")

    # add dimensions as columns
    dims = []
    for r in df.itertuples():
        d = {}
        for dim_filter in json.loads(r.dimensions)["filters"]:  # type: ignore
            d[dim_filter["name"]] = dim_filter["value"]
        dims.append(d)

    df_dims = pd.DataFrame(dims, index=df.index)

    return df.join(df_dims)


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

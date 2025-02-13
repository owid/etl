from typing import Any, Dict, List, Optional, Union

import pandas as pd
from owid.catalog import Table

from etl import multidim
from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Default config for GOOGLE MOBILITY
MOBILITY_CONFIG_DEFAULT = {
    "subtitle": "This data shows how community movement in specific locations has changed relative to the period before the pandemic.",
    "note": "It's not recommended to compare levels across countries; local differences in categories could be misleading.",
    "originUrl": "ourworldindata.org/coronavirus",
    "minTime": "earliest",
    "maxTime": "latest",
    "hideAnnotationFieldsInTitle": {"time": True},
    "addCountryMode": "change-country",
}


def run(dest_dir: str) -> None:
    ds = paths.load_dataset("un_wpp")
    # tb = ds.read("google_mobility")
    tb = ds.read("population")
    engine = get_engine()

    filenames = [
        # "covid.cases.yml",
        # "covid.deaths.yml",
        # "covid.hospital.yml",
        # "covid.vax.yml",
        # "covid.xm.yml",
        # "covid.covax.yml",
        # "covid.models.yml",
        # "covid.xm_models.yml",
        # "covid.vax_breakdowns.yml",
        # "covid.cases_tests.yml",
        # "covid.mobility.yml",
    ]
    # Load YAML file
    for fname in filenames:
        paths.log.info(fname)
        config = paths.load_mdim_config(fname)
        slug = fname_to_slug(fname)
        multidim.upsert_multidim_data_page(
            slug,
            config,
            engine,
            paths.dependencies,
        )

    # Simple multidim
    config = paths.load_mdim_config("covid.population.yml")

    config_new = expand_views(
        config=config,
        tb=tb,
        indicator_name="population",
    )

    multidim.upsert_multidim_data_page(
        "mdd-population",
        config_new,
        engine,
        paths.dependencies,
    )


def fname_to_slug(fname: str) -> str:
    return f"mdd-{fname.replace('.yml', '').replace('.', '-').replace('_', '-')}"


def expand_views(
    config: Dict[str, Any],
    tb: Table,
    indicator_name: Optional[str] = None,
    dimensions: Optional[Union[List[str], Dict[str, Union[List[str], str]]]] = None,
    additional_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Create views from multi-dimensional indicators in table `tb`.

    Parameters:
    -----------
    config : Dict[str, Any]
        Current MDIM configuration loaded from the YAML file (using `paths.load_mdim_config`).
    tb : Table
        Table with the data, including the indicator and its dimensions. The columns in the table are assumed to contain dimensional information. This can be checked in `tb[col].metadata.additional_info["dimensions"]`.
    indicator_name : str | None
        Name of the indicator to use. If None, it assumes there is only one indicator (and will use it), otherwise it will fail.
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
    additional_config : Dict[str, Any] | None
        Additional config fields to add to each view, e.g.
        {"chartTypes": ["LineChart"], "hasMapTab": True, "tab": "map"}

    Example 1: There is only one indicator, we want to expand all dimensions and their values

    ```python
    config = expand_views(
        config=config,
        tb=tb,
    )
    ```

    Example 2: There are multiple indicators, but we focus on 'deaths'. There are dimensions 'sex', 'age' and 'cause' and we want to expand them all completely. We want to show dropdowns in MDIM page in the order sex-age-cause.

    ```python
    config = expand_views(
        config=config,
        tb=tb,
        indicator_name="deaths",
        dimensions=[
            "sex",
            "age",
            "cause",
        ]
    )

    Example 3: Same as Example 2, but for 'cause' we only want to use values 'aids' and 'cancer'.

    ```python
    config = expand_views(
        config=config,
        tb=tb,
        indicator_name="deaths",
        dimensions={
            "sex": "*",
            "age": "*",
            "cause": ["aids", "cancer"],
        }
    )

    Example 3: Same as Example 3, but we want to have very specific sorting of dimension values.

    ```python
    config = expand_views(
        config=config,
        tb=tb,
        indicator_name="deaths",
        dimensions={
            "sex": ["female", "male"],
            "age": ["10", "20", "30", "40", "50", "60", "70", "80", "90", "100+"],
            "cause": ["aids", "cancer"],
        }
    )


    TODO:

        - Add unit testing.
        - Support out-of-the box sorting for dimensions. This could be alphabetically ascending or descending.
            - IDEA: We could do this by passing string values directly to dimensions, e.g. dimensions='alphabetical_desc'
        - Support out-of-the box sorting for dimension values. This could be alphabetically ascending or descending, or numerically ascending or descending.
            - IDEA: We could pass strings as values directly to the keys in dimensions dictionary, e.g. `dimensions={"sex": "alphabetical_desc", "age": "numerical_desc", "cause": ["aids", "cancer"]}`. To some extent, we already support the function "*" (i.e. show all values without sorting).
        - Possible issue: What if there is no intersection for the selected dimensions.
        - Support using charts with 'x', 'size' and 'color' indicators. Also support display settings for each indicator.
        - How can we add tweaks to the dimension description?
            - IDEA: Maybe dimensions argument should rather allow for Dict[str, Dict[str, Any]]. Where for each dimension value we provide description details.
    """
    # GET DATAFRAME WITH INFO ON INDICATOR-DIMENSIONS
    df_dims = _build_df_dims(tb)
    # List with names of indicators and dimensions
    indicator_names = list(df_dims["indicator"].unique())
    dimension_names = [col for col in df_dims.columns if col not in ["indicator", "short_name"]]

    # SANITY CHECKS
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

    # Keep dimensions only for relevant indicator
    df_dims = df_dims.loc[df_dims["indicator"] == indicator_name].drop(columns=["indicator"])

    # Ad-hoc tweak to debug
    df_dims = df_dims.loc[
        df_dims["age"].isin(["0-24", "25-64", "all"])
        & df_dims["sex"].isin(["male", "female"])
        & df_dims["variant"].isin(["low", "high"])
    ]

    # EXPAND DIMENSIONS
    # Support dimension is None
    if dimensions is None:
        dimensions = [col for col in df_dims.columns if col not in ["short_name"]]

    # Support dimensions is a list/dict
    config_dimensions = []
    if isinstance(dimensions, (list, dict)):
        # Sanity check: All dimension names should be present in the list or dictionary
        dimensions_missing = set(dimension_names) - set(dimensions)
        if dimensions_missing:
            raise ValueError(f"Missing dimensions: {', '.join(dimensions_missing)}")

        # Add dimension entry and add it to dimensions
        for dim in dimensions:
            # If list, we don't care about dimension value order
            if isinstance(dimensions, list):
                dim_values = list(df_dims[dim].unique())
            # If dictionary, let's use the order (unless '*' is used)!
            else:
                dim_values = dimensions[dim]
                if dim_values == "*":
                    dim_values = list(df_dims[dim].unique())

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
    config["dimensions"] = config_dimensions

    # TODO: Overwrite dimensions if anything is given in metadata

    # EXPAND VIEWS
    config_views = []
    for _, indicator in df_dims.iterrows():
        view = {
            "dimensions": {dim_name: indicator[dim_name] for dim_name in dimension_names},
            "indicators": {
                "y": f"{tb.m.short_name}#{indicator.short_name}",  # TODO: Add support for (i) support "x", "color", "size"; (ii) display settings
            },
        }
        if additional_config:
            view["config"] = additional_config
        config_views.append(view)
    config["views"] = config_views

    return config


def _build_df_dims(tb):
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

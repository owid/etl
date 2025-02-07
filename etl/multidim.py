import json
from itertools import product

import pandas as pd
import yaml
from owid.catalog import Dataset
from sqlalchemy.engine import Engine
from structlog import get_logger

from apps.chart_sync.admin_api import AdminAPI
from etl.config import OWID_ENV
from etl.db import read_sql
from etl.grapher.io import trim_long_variable_name
from etl.helpers import map_indicator_path_to_id
from etl.paths import DATA_DIR

# Initialize logger.
log = get_logger()


def upsert_multidim_data_page(slug: str, config: dict, engine: Engine, dependencies: list[str] = []) -> None:
    """
    :param dependencies: List of dependencies for mdim. In most cases just use `dependencies=paths.dependencies`.
    """

    expand_catalog_paths(config, dependencies=dependencies)

    validate_multidim_config(config, engine)

    # TODO: Improve this. Could also go into etl.helpers.load_mdim_config
    # Change catalogPaths into variable IDs
    if "views" in config:
        views = config["views"]
        for view in views:
            if "config" in view:
                if "sortColumnSlug" in view["config"]:
                    # Check if catalogPath
                    # Map to variable ID
                    raise NotImplementedError("looks like legacy code?")
                    view["config"]["sortColumnSlug"] = str(map_indicator_path_to_id(view["config"]["sortColumnSlug"]))
                if "dimensions" in view["config"]:
                    dimensions = view["config"]["dimensions"]
                    for dim in dimensions:
                        if "variableId" in dim:
                            # Check if catalogPath
                            # Map to variable ID
                            raise NotImplementedError("looks like legacy code?")
                            dim["variableId"] = map_indicator_path_to_id(dim["variableId"])

    # Upsert config via Admin API
    admin_api = AdminAPI(OWID_ENV)
    admin_api.put_mdim_config(slug, config)


def expand_catalog_paths(config: dict, dependencies: list[str]) -> None:
    """
    Expand catalog paths in views to full dataset URIs.

    This function updates the given configuration dictionary in-place by modifying the "y"
    entries under "indicators" in each view. If an entry does not contain a '/',
    it is assumed to be a table name that must be expanded to a full dataset URI based on
    the provided dependencies.

    Args:
        config (dict): Configuration dictionary containing views.
        dependencies (list[str]): List of dependency URIs in the form "data://<path>".
    """

    def _expand(y: str) -> str:
        if "/" in y:
            return y
        else:
            return table_to_dataset_uri[y.split("#")[0]] + "/" + y

    # Get mapping from table names to dataset URIs
    table_to_dataset_uri = {}
    for dep in dependencies:
        if not dep.startswith("data://"):
            continue

        uri = dep.replace("data://", "")
        ds = Dataset(DATA_DIR / uri)
        for table_name in ds.table_names:
            if table_name in table_to_dataset_uri:
                raise ValueError(f"Table name `{table_name}` is not unique in dependencies")
            table_to_dataset_uri[table_name] = uri

    # Go through all views and expand catalog paths
    for view in config["views"]:
        if isinstance(view["indicators"]["y"], list):
            view["indicators"]["y"] = [_expand(y) for y in view["indicators"]["y"]]
        else:
            view["indicators"]["y"] = _expand(view["indicators"]["y"])


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
        if isinstance(view["indicators"]["y"], list):
            indicators += view["indicators"]["y"]
        else:
            indicators.append(view["indicators"]["y"])

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


def expand_views(config: dict, combinations: dict[str, str], table: str, engine: Engine) -> list[dict]:
    """Use dimensions from multidim config file and create views from all possible
    combinations of dimensions. Grapher table must use the same dimensions as the
    multidim config file. If the dimension is missing from groupby, it will be set to "all".

    :params config: multidim config file
    :params combinations: dictionary with dimension names as keys and values as dimension values, use * for all values
        e.g. {"metric": "*", "age": "*", "cause": "All causes"}
    :params table: catalog path of the grapher table
    :params engine: SQLAlchemy engine
    """

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

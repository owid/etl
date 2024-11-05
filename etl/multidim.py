import json

import pandas as pd
import yaml
from sqlalchemy.engine import Engine

from apps.chart_sync.admin_api import AdminAPI
from etl.config import OWID_ENV
from etl.db import read_sql
from etl.helpers import map_indicator_path_to_id


def upsert_multidim_data_page(slug: str, config: dict, engine: Engine) -> None:
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
                    view["config"]["sortColumnSlug"] = str(map_indicator_path_to_id(view["config"]["sortColumnSlug"]))
                if "dimensions" in view["config"]:
                    dimensions = view["config"]["dimensions"]
                    for dim in dimensions:
                        if "variableId" in dim:
                            # Check if catalogPath
                            # Map to variable ID
                            dim["variableId"] = map_indicator_path_to_id(dim["variableId"])

    # Upsert config via Admin API
    admin_api = AdminAPI(OWID_ENV)
    admin_api.put_mdim_config(slug, config)


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

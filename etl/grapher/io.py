"""Functions to interact with our Grapher data. This includes accessing our database and our API.

TODO: This file contains some code that needs some revision:

- Code dealing with entity codes and names:
    - There are different ways that we are getting code-to-name mappings. We should standardize this.
- Code using db_conn (pymysql.Connection objects). We should instead use sessions, or engines (or OWIDEnv)

"""

import concurrent.futures
import io
import warnings
from collections import defaultdict
from http.client import RemoteDisconnected
from typing import Any, Dict, List, Optional, cast
from urllib.error import HTTPError, URLError

import pandas as pd
import pymysql
import requests
import structlog
import validators
from deprecated import deprecated
from owid.catalog import Dataset, Table
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from tenacity import Retrying
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed

from etl import config
from etl.config import OWID_ENV, OWIDEnv
from etl.db import get_connection, read_sql
from etl.files import checksum_str
from etl.grapher import model as gm
from etl.paths import CACHE_DIR, DATA_DIR

log = structlog.get_logger()


##############################################################################################
# Load from DB
##############################################################################################


def load_dataset_uris(
    owid_env: OWIDEnv = OWID_ENV,
) -> List[str]:
    """Get list of dataset URIs from the database."""
    with Session(owid_env.engine) as session:
        datasets = gm.Dataset.load_datasets_uri(session)

    return list(datasets["dataset_uri"])


def load_variables_in_dataset(
    dataset_uri: Optional[List[str]] = None,
    dataset_id: Optional[List[int]] = None,
    owid_env: OWIDEnv = OWID_ENV,
) -> List[gm.Variable]:
    """Load Variable objects that belong to a dataset with URI `dataset_uri`."""
    with Session(owid_env.engine) as session:
        indicators = gm.Variable.load_variables_in_datasets(
            session=session,
            dataset_uris=dataset_uri,
            dataset_ids=dataset_id,
        )

    return indicators


# Load variable object
def load_variable(
    id_or_path: str | int,
    owid_env: OWIDEnv = OWID_ENV,
) -> gm.Variable:
    """Load variable.

    If id_or_path is str, it'll be used as catalog path.
    """
    if not isinstance(id_or_path, str):
        try:
            id_or_path = int(id_or_path)
        except Exception:
            pass

    with Session(owid_env.engine) as session:
        variable = gm.Variable.from_id_or_path(
            session=session,
            id_or_path=id_or_path,
        )

    return variable


# Load variable object
def load_variables(
    ids_or_paths: List[str | int],
    owid_env: OWIDEnv = OWID_ENV,
) -> List[gm.Variable]:
    """Load variable.

    If id_or_path is str, it'll be used as catalog path.
    """
    with Session(owid_env.engine) as session:
        variables = gm.Variable.from_id_or_path(
            session=session,
            id_or_path=ids_or_paths,
        )

    return variables


def filter_indicators_used_in_charts(indicator_ids: List[int]) -> List[int]:
    """Return a list with only the IDs of the variables used in charts."""
    with Session(OWID_ENV.engine) as session:
        indicator_ids_filtered = gm.ChartDimensions.filter_indicators_used_in_charts(
            session=session, indicator_ids=indicator_ids
        )
        return indicator_ids_filtered


##############################################################################################
# Load data/metadata (API)
##############################################################################################


# SINGLE INDICATOR
# Load variable metadata
def load_variable_metadata(
    catalog_path: Optional[str] = None,
    variable_id: Optional[int] = None,
    variable: Optional[gm.Variable] = None,
    owid_env: OWIDEnv = OWID_ENV,
) -> Dict[str, Any]:
    """Get metadata for an indicator based on its catalog path or variable id.

    Parameters
    ----------
    catalog_path : str, optional
        The path to the indicator in the catalog.
    variable_id : int, optional
        The ID of the indicator.
    variable : Variable, optional
        The indicator object.
    """
    # Get variable
    variable = ensure_load_variable(catalog_path, variable_id, variable, owid_env)

    # Get metadata
    metadata = variable.get_metadata()

    return metadata


def load_variable_data(
    catalog_path: Optional[str] = None,
    variable_id: Optional[int] = None,
    variable: Optional[gm.Variable] = None,
    owid_env: OWIDEnv = OWID_ENV,
    set_entity_names: bool = True,
) -> pd.DataFrame:
    """Get data for an indicator based on its catalog path or variable id.

    Parameters
    ----------
    cataslog_path : str, optional
        The path to the indicator in the catalog.
    variable_id : int, optional
        The ID of the indicator.
    variable : Variable, optional
        The indicator object.

    """

    # Get variable
    variable = ensure_load_variable(catalog_path, variable_id, variable, owid_env)

    if set_entity_names:
        # Get data
        with Session(owid_env.engine) as session:
            df = variable.get_data(session=session)
    else:
        df = variable.get_data()

    return df


def ensure_load_variable(
    catalog_path: Optional[str] = None,
    variable_id: Optional[int] = None,
    variable: Optional[gm.Variable] = None,
    owid_env: OWIDEnv = OWID_ENV,
) -> gm.Variable:
    if variable is None:
        if catalog_path is not None:
            variable = load_variable(id_or_path=catalog_path, owid_env=owid_env)
        elif variable_id is not None:
            variable = load_variable(id_or_path=variable_id, owid_env=owid_env)
        else:
            raise ValueError("Either catalog_path, variable_id or variable must be provided")
    return variable


##############################################################################################
# More optimized API access
# Most useful for bulk operations
# from apps.backport.datasync.data_metadata
##############################################################################################


def load_variables_data(
    catalog_paths: Optional[List[str]] = None,
    variable_ids: Optional[List[int]] = None,
    variables: Optional[List[gm.Variable]] = None,
    owid_env: OWIDEnv = OWID_ENV,
    workers: int = 1,
    value_as_str: bool = True,
) -> pd.DataFrame:
    """Get data for a list of indicators based on their catalog path or variable id.

    Priority: catalog_paths > variable_ids > variables

    Parameters
    ----------
    cataslog_path : str, optional
        The path to the indicator in the catalog.
    variable_id : int, optional
        The ID of the indicator.
    variable : Variable, optional
        The indicator object.

    """
    # Get variable IDs
    variable_ids = _ensure_variable_ids(owid_env.engine, catalog_paths, variable_ids, variables)

    # Get variable
    df = variable_data_df_from_s3(
        owid_env.engine,
        variable_ids=variable_ids,
        workers=workers,
        value_as_str=value_as_str,
    )

    return df


def load_variables_metadata(
    catalog_paths: Optional[List[str]] = None,
    variable_ids: Optional[List[int]] = None,
    variables: Optional[List[gm.Variable]] = None,
    owid_env: OWIDEnv = OWID_ENV,
    workers: int = 1,
) -> List[Dict[str, Any]]:
    """Get metadata for a list of indicators based on their catalog path or variable id.

    Priority: catalog_paths > variable_ids > variables

    Parameters
    ----------
    catalog_path : str, optional
        The path to the indicator in the catalog.
    variable_id : int, optional
        The ID of the indicator.
    variable : Variable, optional
        The indicator object.
    """

    # Get variable IDs
    variable_ids = _ensure_variable_ids(owid_env.engine, catalog_paths, variable_ids, variables)

    metadata = variable_metadata_df_from_s3(
        variable_ids=variable_ids,
        workers=workers,
        env=owid_env,
    )

    return metadata


def _ensure_variable_ids(
    engine: Engine,
    catalog_paths: Optional[List[str]] = None,
    variable_ids: Optional[List[int]] = None,
    variables: Optional[List[gm.Variable]] = None,
) -> List[int]:
    if catalog_paths is not None:
        with Session(engine) as session:
            mapping = gm.Variable.catalog_paths_to_variable_ids(session, catalog_paths=catalog_paths)
        variable_ids = [int(i) for i in mapping.values()]
    elif (variable_ids is None) and (variables is not None):
        variable_ids = [variable.id for variable in variables]

    if variable_ids is None:
        raise ValueError("Either catalog_paths, variable_ids or variables must be provided")

    return variable_ids


def variable_data_df_from_s3(
    engine: Engine,
    variable_ids: List[int] = [],
    workers: Optional[int] = 1,
    value_as_str: bool = True,
) -> pd.DataFrame:
    """Fetch data from S3 and add entity code and name from DB."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(_fetch_data_df_from_s3, variable_ids))

    if isinstance(results, list) and all(isinstance(df, pd.DataFrame) for df in results):
        df = pd.concat(cast(List[pd.DataFrame], results))
    else:
        raise TypeError(f"results must be a list of pd.DataFrame, got {type(results)}")

    # we work with strings and convert to specific types later
    if value_as_str:
        df["value"] = df["value"].astype("string")

    with Session(engine) as session:
        res = add_entity_code_and_name(session, df)
        return res


def _fetch_response(method: str, url: str):
    """Helper function to perform HTTP requests with retries and centralized exception handling."""
    try:
        for attempt in Retrying(
            wait=wait_fixed(2),
            stop=stop_after_attempt(3),
            retry=retry_if_exception_type((URLError, RemoteDisconnected, requests.exceptions.RequestException)),
        ):
            with attempt:
                response = requests.request(method, url)
                response.raise_for_status()
                return response
    except HTTPError as e:
        # No data on S3
        if e.response.status_code == 404:
            return None
        else:
            raise
    except (URLError, RemoteDisconnected, requests.exceptions.RequestException):
        raise


def _fetch_data_df_from_s3(variable_id: int):
    cache_dir = CACHE_DIR / "variable_data"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_filename = cache_dir / f"{variable_id}.json"
    etag_filename = cache_dir / f"{variable_id}.etag"

    url = config.variable_data_url(variable_id)

    # Check if cached data exists
    if cache_filename.exists() and etag_filename.exists():
        # Read stored ETag
        stored_etag = etag_filename.read_text()
    else:
        stored_etag = None

    # Get current ETag from server
    response = _fetch_response("HEAD", url)
    if response is None:
        return pd.DataFrame(columns=["variableId", "entityId", "year", "value"])
    current_etag = response.headers.get("ETag")

    # Compare ETags
    if stored_etag and current_etag and stored_etag == current_etag:
        # ETag matches, load from cache
        data_df = pd.read_json(cache_filename)
    else:
        # Fetch new data
        response = _fetch_response("GET", url)
        if response is None:
            return pd.DataFrame(columns=["variableId", "entityId", "year", "value"])
        # Save response text to cache
        cache_filename.write_text(response.text, encoding="utf-8")
        # Save new ETag
        if current_etag:
            etag_filename.write_text(current_etag)
        elif etag_filename.exists():
            etag_filename.unlink()
        data_df = pd.read_json(io.StringIO(response.text))

    # Process DataFrame
    data_df = data_df.rename(
        columns={
            "entities": "entityId",
            "values": "value",
            "years": "year",
        }
    ).assign(variableId=variable_id)
    return data_df


def add_entity_code_and_name(session: Session, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        df["entityName"] = []
        df["entityCode"] = []
        return df

    unique_entities = df["entityId"].unique()

    entities = _fetch_entities(session, list(unique_entities))

    if set(unique_entities) - set(entities.entityId):
        missing_entities = set(unique_entities) - set(entities.entityId)
        raise ValueError(f"Missing entities in the database: {missing_entities}")

    return pd.merge(df, entities.astype({"entityName": "category", "entityCode": "category"}), on="entityId")


def _fetch_entities(session: Session, entity_ids: List[int]) -> pd.DataFrame:
    # Query entities from the database
    q = """
    SELECT
        id AS entityId,
        name AS entityName,
        code AS entityCode
    FROM entities
    WHERE id in %(entity_ids)s
    """
    return read_sql(q, session, params={"entity_ids": entity_ids})


def variable_metadata_df_from_s3(
    variable_ids: List[int] = [],
    workers: int = 1,
    env: OWIDEnv | None = None,
) -> List[Dict[str, Any]]:
    """Fetch data from S3 and add entity code and name from DB."""
    args = [variable_ids]
    if env:
        args += [[env for _ in range(len(variable_ids))]]

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(_fetch_metadata_from_s3, *args))

    if not (isinstance(results, list) and all(isinstance(res, dict) for res in results)):
        raise TypeError(f"results must be a list of dictionaries, got {type(results)}")

    return results  # type: ignore


def _fetch_metadata_from_s3(variable_id: int, env: OWIDEnv | None = None) -> Dict[str, Any]:
    if env is not None:
        url = env.indicator_metadata_url(variable_id)
    else:
        url = config.variable_metadata_url(variable_id)

    response = _fetch_response("GET", url)
    if response is None:
        return {}
    else:
        return response.json()


def load_entity_mapping(entity_ids: Optional[List[int]] = None, owid_env: OWIDEnv = OWID_ENV) -> Dict[int, str]:
    # Fetch the mapping of entity ids to names.
    with Session(owid_env.engine) as session:
        entity_id_to_name = gm.Entity.load_entity_mapping(session=session, entity_ids=entity_ids)

    return entity_id_to_name


def variable_data_table_from_catalog(
    engine: Engine, variables: Optional[List[gm.Variable]] = None, variable_ids: Optional[List[int | str]] = None
) -> Table:
    """Load all variables for a given dataset from local catalog."""
    if variable_ids:
        assert not variables, "Only one of variables or variable_ids must be provided"
        with Session(engine) as session:
            variables = gm.Variable.from_id_or_path(session, variable_ids, columns=["id", "shortName", "dimensions"])
    elif variables:
        assert not variable_ids, "Only one of variables or variable_ids must be provided"
    else:
        raise ValueError("Either variables or variable_ids must be provided")

    to_read = defaultdict(list)

    # Group variables by dataset and table
    # TODO: use CatalogPath object
    for variable in variables:
        assert variable.catalogPath, f"Variable {variable.id} has no catalogPath"
        path, short_name = variable.catalogPath.split("#")
        ds_path, table_name = path.rsplit("/", 1)
        to_read[(ds_path, table_name)].append(variable)

    # Read the table and load all its variables
    tbs = []
    for (ds_path, table_name), variables in to_read.items():
        try:
            tb = Dataset(DATA_DIR / ds_path).read(table_name, safe_types=False)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Dataset {ds_path} not found in local catalog.") from e

        if "date" in tb.columns:
            year_or_date = "date"
        elif "year" in tb.columns:
            year_or_date = "year"
        else:
            raise ValueError(f"Table {table_name} has no 'date' or 'year' column")

        dim_names = [k for k in tb.metadata.primary_key if k not in ("country", year_or_date)]

        # Simple case with no dimensions
        if not dim_names:
            col_mapping = {"country": "country", year_or_date: year_or_date}
            for col in set(tb.columns) - {"country", year_or_date}:
                # Variable names in MySQL are trimmed to 255 characters
                name = trim_long_variable_name(col)
                matches = [variable for variable in variables if name == variable.shortName]
                if matches:
                    col_mapping[col] = matches[0].id  # type: ignore

            tb = tb[col_mapping.keys()]
            tb.columns = col_mapping.values()
            tbs.append(tb.set_index(["country", year_or_date]))

        # Dimensional case
        else:
            # NOTE: example of `filters`
            # [
            #     {'name': 'question', 'value': 'mh1 - Importance of mental health for well-being'},
            #     {'name': 'answer', 'value': 'As important'},
            #     {'name': 'gender', 'value': 'all'},
            #     {'name': 'age_group', 'value': '15-29'}
            # ]
            dim_names = [k for k in tb.metadata.primary_key if k not in ("country", year_or_date)]
            tb_pivoted = tb.pivot(index=["country", year_or_date], columns=dim_names)

            labels = []
            for variable in variables:
                if not variable.dimensions:
                    label = [variable.shortName] + [None] * len(dim_names)
                    # assert variable.dimensions, f"Variable {variable.id} has no dimensions"
                else:
                    # Construct label for multidim columns
                    label = [variable.dimensions["originalShortName"]]
                    for dim_name in dim_names:
                        for f in variable.dimensions["filters"]:
                            if f["name"] == dim_name:
                                label.append(f["value"])
                                break
                        else:
                            label.append(None)  # type: ignore
                labels.append(label)

            tb = tb_pivoted.loc[:, labels]

            tb.columns = [variable.id for variable in variables]
            tbs.append(tb)

    # NOTE: this can be pretty slow for datasets with a lot of tables
    return pd.concat(tbs, axis=1).reset_index()  # type: ignore


#######################################################################################################

# TO BE REVIEWED:
# This is code that could be deprecated / removed?
# TODO: replace usage of db_conn (pymysql.Connection) with engine (sqlalchemy.engine.Engine) or OWIDEnv
#######################################################################################################


def get_dataset_id(
    dataset_name: str, db_conn: Optional[pymysql.Connection] = None, version: Optional[str] = None
) -> Any:
    """Get the dataset ID of a specific dataset name from database.

    If more than one dataset is found for the same name, or if no dataset is found, an error is raised.

    Parameters
    ----------
    dataset_name : str
        Dataset name.
    db_conn : pymysql.Connection
        Connection to database. Defaults to None, in which case a default connection is created (uses etl.config).
    version : str
        ETL version of the dataset. This is necessary when multiple datasets have the same title. In such a case, if
        version is not given, the function will raise an error.

    Returns
    -------
    dataset_id : int
        Dataset ID.

    """
    if db_conn is None:
        db_conn = get_connection()

    query = f"""
        SELECT id
        FROM datasets
        WHERE name = '{dataset_name}'
    """

    if version:
        query += f" AND version = '{version}'"

    with db_conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

    assert len(result) == 1, f"Ambiguous or unknown dataset name '{dataset_name}'"
    dataset_id = result[0][0]
    return dataset_id


@deprecated("This function is deprecated. Its logic will be soon moved to etl.grapher.model.Dataset.")
def get_variables_in_dataset(
    dataset_id: int, only_used_in_charts: bool = False, db_conn: Optional[pymysql.Connection] = None
) -> Any:
    """Get all variables data for a specific dataset ID from database.

    Parameters
    ----------
    dataset_id : int
        Dataset ID.
    only_used_in_charts : bool
        True to select variables only if they have been used in at least one chart. False to select all variables.
    db_conn : pymysql.Connection
        Connection to database. Defaults to None, in which case a default connection is created (uses etl.config).

    Returns
    -------
    variables_data : pd.DataFrame
        Variables data for considered dataset.

    """
    if db_conn is None:
        db_conn = get_connection()

    query = f"""
        SELECT *
        FROM variables
        WHERE datasetId = {dataset_id}
    """
    if only_used_in_charts:
        query += """
            AND id IN (
                SELECT DISTINCT variableId
                FROM chart_dimensions
            )
        """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        variables_data = pd.read_sql(query, con=db_conn)
    return variables_data


def get_all_datasets(archived: bool = True, db_conn: Optional[pymysql.Connection] = None) -> pd.DataFrame:
    """Get all datasets in database.

    Parameters
    ----------
    db_conn : pymysql.connections.Connection
        Connection to database. Defaults to None, in which case a default connection is created (uses etl.config).

    Returns
    -------
    datasets : pd.DataFrame
        All datasets in database. Table with three columns: dataset ID, dataset name, dataset namespace.
    """
    if db_conn is None:
        db_conn = get_connection()

    query = " SELECT namespace, name, id, updatedAt, isArchived FROM datasets"
    if not archived:
        query += " WHERE isArchived = 0"
    datasets = pd.read_sql(query, con=db_conn)
    return datasets.sort_values(["name", "namespace"])


def get_info_for_etl_datasets(db_conn: Optional[pymysql.Connection] = None) -> pd.DataFrame:
    """Get information for datasets that have variables with an ETL path.

    This function returns a dataframe with the following columns:
    - dataset_id: ID of the dataset.
    - dataset_name: Name of the dataset.
    - etl_path: ETL path of the dataset.
    - is_archived: Whether the dataset is archived.
    - is_private: Whether the dataset is private.
    - chart_ids: List of chart ids that use variables from the dataset.
    - chart_slugs: List of tuples (chart_id, chart_slug) that use variables from the dataset.
    - views_7d: List of tuples (chart_id, views_7d) for the last 7 days.
    - views_14d: List of tuples (chart_id, views_14d) for the last 14 days.
    - views_365d: List of tuples (chart_id, views_365d) for the last 365 days.
    - update_period_days: Update period of the dataset.
    """
    if db_conn is None:
        db_conn = get_connection()

    # First, increase the GROUP_CONCAT limit, to avoid the list of chart ids to be truncated.
    GROUP_CONCAT_MAX_LEN = 4096
    cursor = db_conn.cursor()
    cursor.execute(f"SET SESSION group_concat_max_len = {GROUP_CONCAT_MAX_LEN};")
    db_conn.commit()

    query = """\
    SELECT
        q1.datasetId AS dataset_id,
        d.name AS dataset_name,
        q1.etlPath AS etl_path,
        d.isArchived AS is_archived,
        d.isPrivate AS is_private,
        q2.chartIds AS chart_ids,
        q2.updatePeriodDays AS update_period_days
    FROM
        (SELECT
            datasetId,
            MIN(catalogPath) AS etlPath
        FROM
            variables
        WHERE
            catalogPath IS NOT NULL
        GROUP BY
            datasetId) q1
    LEFT JOIN
        (SELECT
            d.id AS datasetId,
            d.isArchived,
            d.isPrivate,
            d.updatePeriodDays,
            GROUP_CONCAT(DISTINCT c.id) AS chartIds
        FROM
            datasets d
            JOIN variables v ON v.datasetId = d.id
            JOIN chart_dimensions cd ON cd.variableId = v.id
            JOIN charts c ON c.id = cd.chartId
            JOIN chart_configs cc ON c.configId = cc.id
        WHERE
            json_extract(cc.full, "$.isPublished") = TRUE
        GROUP BY
            d.id) q2
        ON q1.datasetId = q2.datasetId
    JOIN
        datasets d ON q1.datasetId = d.id
    ORDER BY
        q1.datasetId ASC;

    """

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        df = pd.read_sql(query, con=db_conn)

    if max([len(row) for row in df["chart_ids"] if row is not None]) == GROUP_CONCAT_MAX_LEN:
        log.error(
            f"The value of group_concat_max_len (set to {GROUP_CONCAT_MAX_LEN}) has been exceeded."
            "This means that the list of chart ids will be incomplete in some cases. Consider increasing it."
        )

    # Get mapping of chart ids to slugs.
    chart_id_to_slug = get_charts_slugs(db_conn=db_conn).set_index("chart_id")["chart_slug"].to_dict()

    # Instead of having a string of chart ids, make chart_ids a column with lists of integers.
    df["chart_ids"] = [
        [int(chart_id) for chart_id in chart_ids.split(",")] if chart_ids else [] for chart_ids in df["chart_ids"]
    ]
    # Add a column with lists of chart slugs.
    # For each row, it will be a list of tuples (chart_id, chart_slug),
    # e.g. [(123, "chart-slug"), (234, "another-chart-slug"), ...].
    df["chart_slugs"] = [
        [(chart_id, chart_id_to_slug[chart_id]) for chart_id in chart_ids] if chart_ids else []
        for chart_ids in df["chart_ids"]
    ]

    # Add chart analytics.
    views_df = get_charts_views(db_conn=db_conn).set_index("slug")
    # Create a column for each of the views metrics.
    # For each row, it will be a list of tuples (chart_id, views),
    # e.g. [(123, 1000), (234, 2000), ...].
    for metric in views_df.columns:
        df[metric] = [
            [
                (chart_id, views_df[metric][chart_id_to_slug[chart_id]])
                for chart_id in chart_ids
                if chart_id_to_slug[chart_id] in views_df.index
            ]
            if chart_ids
            else []
            for chart_ids in df["chart_ids"]
        ]

    # Make is_archived and is_private boolean columns.
    df["is_archived"] = df["is_archived"].astype(bool)
    df["is_private"] = df["is_private"].astype(bool)

    # Sanity check.
    unknown_channels = set([etl_path.split("/")[0] for etl_path in set(df["etl_path"])]) - {"grapher"}
    if len(unknown_channels) > 0:
        log.error(
            "Variables in grapher DB are expected to come only from ETL grapher channel, "
            f"but other channels were found: {unknown_channels}"
        )

    return df


def get_charts_slugs(db_conn: Optional[pymysql.Connection] = None) -> pd.DataFrame:
    if db_conn is None:
        db_conn = get_connection()

    # Get a dataframe chart_id,char_slug, for all charts that have variables with an ETL path.
    query = """\
    SELECT
        c.id AS chart_id,
        cc.slug AS chart_slug
    FROM charts c
    JOIN chart_configs cc ON c.configId = cc.id
    LEFT JOIN chart_dimensions cd ON c.id = cd.chartId
    LEFT JOIN variables v ON cd.variableId = v.id
    WHERE
        v.catalogPath IS NOT NULL
    ORDER BY
        c.id ASC;
    """

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        df = pd.read_sql(query, con=db_conn)

    # Remove duplicated rows.
    df = df.drop_duplicates().reset_index(drop=True)

    if len(df[df.duplicated(subset="chart_id")]) > 0:
        log.warning("There are duplicated chart ids in the chart_ids and slugs table.")

    return df


def get_charts_views(db_conn: Optional[pymysql.Connection] = None) -> pd.DataFrame:
    if db_conn is None:
        db_conn = get_connection()

    # Assumed base url for all charts.
    base_url = "https://ourworldindata.org/grapher/"

    # Note that for now we extract data for all dates.
    # It seems that the table only has data for the last day.
    query = f"""\
    SELECT
        url,
        views_7d,
        views_14d,
        views_365d
    FROM
        analytics_pageviews
    WHERE
        url LIKE '{base_url}%';
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        df = pd.read_sql(query, con=db_conn)

    # For some reason, there are spurious urls, clean some of them.
    # Note that validators.url() returns a ValidationError object (instead of False) when the url has spaces.
    is_url_invalid = [(validators.url(url) is False) or (" " in url) for url in df["url"]]
    df = df.drop(df[is_url_invalid].index).reset_index(drop=True)

    # Note that some of the returned urls may still be invalid, for example "https://ourworldindata.org/grapher/132".

    # Add chart slug.
    df["slug"] = [url.replace(base_url, "") for url in df["url"]]

    # Remove url.
    df = df.drop(columns=["url"], errors="raise")

    if len(df[df.duplicated(subset="slug")]) > 0:
        log.warning("There are duplicated slugs in the chart analytics table.")

    return df


def get_dataset_charts(dataset_ids: List[str], db_conn: Optional[pymysql.Connection] = None) -> pd.DataFrame:
    if db_conn is None:
        db_conn = get_connection()

    dataset_ids_str = ", ".join(map(str, dataset_ids))

    query = f"""
    SELECT
        d.id AS dataset_id,
        d.name AS dataset_name,
        q2.chartIds AS chart_ids
    FROM
        (SELECT
            d.id,
            d.name
        FROM
            datasets d
        WHERE
            d.id IN ({dataset_ids_str})) d
    LEFT JOIN
        (SELECT
            v.datasetId,
            GROUP_CONCAT(DISTINCT c.id) AS chartIds
        FROM
            variables v
            JOIN chart_dimensions cd ON cd.variableId = v.id
            JOIN charts c ON c.id = cd.chartId
        WHERE
            v.datasetId IN ({dataset_ids_str})
        GROUP BY
            v.datasetId) q2
        ON d.id = q2.datasetId
    ORDER BY
        d.id ASC;
    """

    # First, increase the GROUP_CONCAT limit, to avoid the list of chart ids to be truncated.
    with db_conn.cursor() as cursor:
        cursor.execute("SET SESSION group_concat_max_len = 10000;")

    if len(dataset_ids) == 0:
        return pd.DataFrame({"dataset_id": [], "dataset_name": [], "chart_ids": []})

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        df = pd.read_sql(query, con=db_conn)

    # Instead of having a string of chart ids, make chart_ids a column with lists of integers.
    df["chart_ids"] = [
        [int(chart_id) for chart_id in chart_ids.split(",")] if chart_ids else [] for chart_ids in df["chart_ids"]
    ]

    return df


def get_variables_data(
    filter: Optional[Dict[str, Any]] = None,
    condition: Optional[str] = "OR",
    db_conn: Optional[pymysql.Connection] = None,
) -> pd.DataFrame:
    """Get data from variables table, given a certain condition.

    Parameters
    ----------
    filter : Optional[Dict[str, Any]], optional
        Filter to apply to the data, which must contain a field name and a list of field values,
        e.g. {"id": [123456, 234567, 345678]}.
        In principle, multiple filters can be given.
    condition : Optional[str], optional
        In case multiple filters are given, this parameter specifies whether the output filters should be the union
        ("OR") or the intersection ("AND").
    db_conn : pymysql.Connection
        Connection to database. Defaults to None, in which case a default connection is created (uses etl.config).

    Returns
    -------
    df : pd.DataFrame
        Variables data.

    """
    # NOTE: This function should be optimized. Instead of fetching data for each filter, their conditions should be
    # combined with OR or AND before executing the query.

    # Initialize an empty dataframe.
    if filter is not None:
        df = pd.DataFrame({"id": []}).astype({"id": int})
        for field_name, field_values in filter.items():
            _df = _get_variables_data_with_filter(field_name=field_name, field_values=field_values, db_conn=db_conn)
            if condition == "OR":
                df = pd.concat([df, _df], axis=0)
            elif condition == "AND":
                df = pd.merge(df, _df, on="id", how="inner")
            else:
                raise ValueError(f"Invalid condition: {condition}")
    else:
        # Fetch data for all variables.
        df = _get_variables_data_with_filter(db_conn=db_conn)

    return df


def _get_variables_data_with_filter(
    field_name: Optional[str] = None,
    field_values: Optional[List[Any]] = None,
    db_conn: Optional[pymysql.Connection] = None,
) -> Any:
    if db_conn is None:
        db_conn = get_connection()

    if field_values is None:
        field_values = []

    # Construct the SQL query with a placeholder for each value in the list.
    query = "SELECT * FROM variables"

    if (field_name is not None) and (len(field_values) > 0):
        query += f"\nWHERE {field_name} IN ({', '.join(['%s'] * len(field_values))});"

    # Execute the query.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        variables_data = pd.read_sql(query, con=db_conn, params=field_values)

    assert set(variables_data[field_name]) <= set(field_values), f"Unexpected values for {field_name}."

    # Warn about values that were not found.
    missing_values = set(field_values) - set(variables_data[field_name])
    if len(missing_values) > 0:
        log.warning(f"Values of {field_name} not found in database: {missing_values}")

    return variables_data


def trim_long_variable_name(short_name: str) -> str:
    """Trim long variable name to 255 characters and add a hash to make it unique."""
    if len(short_name) > 255:
        unique_hash = f"_{checksum_str(short_name)}"
        return short_name[: (255 - len(unique_hash))] + unique_hash
    else:
        return short_name

import functools
import os
import warnings
from typing import Any, Dict, Optional
from urllib.parse import quote

import pandas as pd
import pymysql
import structlog
from deprecated import deprecated
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import Session

from etl import config

log = structlog.get_logger()


def can_connect(conf: Optional[Dict[str, Any]] = None) -> bool:
    try:
        get_connection(conf=conf)
        return True
    except pymysql.OperationalError:
        return False


@deprecated("This function is deprecated. Instead, look at using etl.db.read_sql function.")
def get_connection(conf: Optional[Dict[str, Any]] = None) -> pymysql.Connection:
    "Connect to the Grapher database."
    cf: Any = dict_to_object(conf) if conf else config
    return pymysql.connect(
        db=cf.DB_NAME,
        host=cf.DB_HOST,
        port=cf.DB_PORT,
        user=cf.DB_USER,
        password=cf.DB_PASS,
        charset="utf8mb4",
        autocommit=True,
    )


def get_session(**kwargs) -> Session:
    """Get session with defaults."""
    return Session(get_engine(**kwargs))


@functools.cache
def _get_engine_cached(cf: Any, pid: int) -> Engine:
    return create_engine(
        f"mysql+pymysql://{cf.DB_USER}:{quote(cf.DB_PASS)}@{cf.DB_HOST}:{cf.DB_PORT}/{cf.DB_NAME}",
        pool_size=30,  # Increase the pool size to allow higher GRAPHER_WORKERS
        max_overflow=30,  # Increase the max overflow limit to allow higher GRAPHER_WORKERS
    )


def get_engine(conf: Optional[Dict[str, Any]] = None) -> Engine:
    cf: Any = dict_to_object(conf) if conf else config
    # pid in memoization makes sure every process gets its own Engine
    pid = os.getpid()
    return _get_engine_cached(cf, pid)


def get_engine_async(conf: Optional[Dict[str, Any]] = None) -> AsyncEngine:
    cf: Any = dict_to_object(conf) if conf else config
    engine = create_async_engine(
        f"mysql+aiomysql://{cf.DB_USER}:{quote(cf.DB_PASS)}@{cf.DB_HOST}:{cf.DB_PORT}/{cf.DB_NAME}",
        pool_size=30,  # Increase pool size
        max_overflow=50,  # Increase overflow limit
    )
    return engine


def get_dataset_id(
    dataset_name: str,
    db_conn: Optional[pymysql.Connection] = None,
    version: Optional[str] = None,
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


def get_variables_in_dataset(
    dataset_id: int,
    only_used_in_charts: bool = False,
    db_conn: Optional[pymysql.Connection] = None,
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

    query = " SELECT namespace, name, id, updatedAt FROM datasets"
    if not archived:
        query += " WHERE isArchived = 0"
    datasets = pd.read_sql(query, con=db_conn)
    return datasets.sort_values(["name", "namespace"])


def dict_to_object(d):
    return type("DynamicObject", (object,), d)()


def read_sql(sql: str, engine: Optional[Engine | Session] = None, *args, **kwargs) -> pd.DataFrame:
    """Wrapper around pd.read_sql that creates a connection and closes it after reading the data.
    This adds overhead, so if you need performance, reuse the same connection and cursor.
    """
    engine = engine or get_engine()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        if isinstance(engine, Engine):
            with engine.connect() as con:
                return pd.read_sql(sql, con, *args, **kwargs)
        elif isinstance(engine, Session):
            return pd.read_sql(sql, engine.bind, *args, **kwargs)
        else:
            raise ValueError(f"Unsupported engine type {type(engine)}")


def to_sql(
    df: pd.DataFrame,
    name: str,
    engine: Optional[Engine | Session] = None,
    *args,
    **kwargs,
):
    """Wrapper around pd.to_sql that creates a connection and closes it after reading the data.
    This adds overhead, so if you need performance, reuse the same connection and cursor.
    """
    engine = engine or get_engine()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        if isinstance(engine, Engine):
            with engine.connect() as con:
                return df.to_sql(name, con, *args, **kwargs)
        elif isinstance(engine, Session):
            return df.to_sql(name, engine.bind, *args, **kwargs)
        else:
            raise ValueError(f"Unsupported engine type {type(engine)}")


def production_or_master_engine() -> Engine:
    """Return the production engine if available, otherwise connect to staging-site-master."""
    if config.OWID_ENV.env_remote == "production":
        return config.OWID_ENV.get_engine()
    elif config.ENV_FILE_PROD:
        return config.OWIDEnv.from_env_file(config.ENV_FILE_PROD).get_engine()
    else:
        log.warning("ENV file doesn't connect to production DB, comparing against staging-site-master")
        return config.OWIDEnv.from_staging("master").get_engine()

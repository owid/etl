import traceback
import warnings
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Dict, Optional
from urllib.parse import quote

import MySQLdb
import pandas as pd
import structlog
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlmodel import Session

from etl import config
from etl.db_utils import DBUtils

log = structlog.get_logger()


def get_connection(conf: Optional[Dict[str, Any]] = None) -> MySQLdb.Connection:
    "Connect to the Grapher database."
    cf: Any = dict_to_object(conf) if conf else config
    return MySQLdb.connect(
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


def get_engine(conf: Optional[Dict[str, Any]] = None) -> Engine:
    cf: Any = dict_to_object(conf) if conf else config

    return create_engine(
        f"mysql://{cf.DB_USER}:{quote(cf.DB_PASS)}@{cf.DB_HOST}:{cf.DB_PORT}/{cf.DB_NAME}",
        pool_size=30,  # Increase the pool size to allow higher GRAPHER_WORKERS
        max_overflow=30,  # Increase the max overflow limit to allow higher GRAPHER_WORKERS
    )


@contextmanager
def open_db() -> Generator[DBUtils, None, None]:
    connection = None
    cursor = None
    try:
        connection = get_connection()
        connection.autocommit(False)
        cursor = connection.cursor()
        yield DBUtils(cursor)
        connection.commit()
    except Exception as e:
        log.error(f"Error encountered during import: {e}")
        log.error("Rolling back changes...")
        if connection:
            connection.rollback()
        if config.DEBUG:
            traceback.print_exc()
        raise e
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def get_dataset_id(dataset_name: str, db_conn: Optional[MySQLdb.Connection] = None) -> Any:
    """Get the dataset ID of a specific dataset name from database.

    If more than one dataset is found for the same name, or if no dataset is found, an error is raised.

    Parameters
    ----------
    dataset_name : str
        Dataset name.
    db_conn : MySQLdb.Connection
        Connection to database. Defaults to None, in which case a default connection is created (uses etl.config).

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
    with db_conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

    assert len(result) == 1, f"Ambiguous or unknown dataset name '{dataset_name}'"
    dataset_id = result[0][0]
    return dataset_id


def get_variables_in_dataset(
    dataset_id: int, only_used_in_charts: bool = False, db_conn: Optional[MySQLdb.Connection] = None
) -> Any:
    """Get all variables data for a specific dataset ID from database.

    Parameters
    ----------
    dataset_id : int
        Dataset ID.
    only_used_in_charts : bool
        True to select variables only if they have been used in at least one chart. False to select all variables.
    db_conn : MySQLdb.Connection
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


def get_all_datasets(archived: bool = True, db_conn: Optional[MySQLdb.Connection] = None) -> pd.DataFrame:
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

    query = " SELECT namespace, name, id FROM datasets"
    if not archived:
        query += " WHERE isArchived = 0"
    datasets = pd.read_sql(query, con=db_conn)
    return datasets.sort_values(["name", "namespace"])


def dict_to_object(d):
    return type("DynamicObject", (object,), d)()

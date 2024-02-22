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


def can_connect(conf: Optional[Dict[str, Any]] = None) -> bool:
    try:
        get_connection(conf=conf)
        return True
    except MySQLdb.OperationalError:
        return False


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


def get_info_for_etl_datasets(db_conn: Optional[MySQLdb.Connection] = None) -> pd.DataFrame:
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
        WHERE
            json_extract(c.config, "$.isPublished") = TRUE
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

    # Instead of having a string of chart ids, make chart_ids a column with lists of integers.
    df["chart_ids"] = [
        [int(chart_id) for chart_id in chart_ids.split(",")] if chart_ids else [] for chart_ids in df["chart_ids"]
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

    # Create a column with the step name.
    # First assume all steps are public (hence starting with "data://").
    # Then edit private steps so they start with "data-private://".
    df["step"] = ["data://" + "/".join(etl_path.split("#")[0].split("/")[:-1]) for etl_path in df["etl_path"]]
    df.loc[df["is_private"], "step"] = df[df["is_private"]]["step"].str.replace("data://", "data-private://")

    return df

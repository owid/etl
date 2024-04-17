import warnings
from typing import Any, Dict, List, Optional, cast
from urllib.parse import quote

import MySQLdb
import pandas as pd
import structlog
import validators
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlmodel import Session

from etl import config

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

    return cast(
        Engine,
        create_engine(
            f"mysql://{cf.DB_USER}:{quote(cf.DB_PASS)}@{cf.DB_HOST}:{cf.DB_PORT}/{cf.DB_NAME}",
            pool_size=30,  # Increase the pool size to allow higher GRAPHER_WORKERS
            max_overflow=30,  # Increase the max overflow limit to allow higher GRAPHER_WORKERS
        ),
    )


def get_dataset_id(
    dataset_name: str, db_conn: Optional[MySQLdb.Connection] = None, version: Optional[str] = None
) -> Any:
    """Get the dataset ID of a specific dataset name from database.

    If more than one dataset is found for the same name, or if no dataset is found, an error is raised.

    Parameters
    ----------
    dataset_name : str
        Dataset name.
    db_conn : MySQLdb.Connection
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


def _get_variables_data_with_filter(
    field_name: Optional[str] = None,
    field_values: Optional[List[Any]] = None,
    db_conn: Optional[MySQLdb.Connection] = None,
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
    db_conn: Optional[MySQLdb.Connection] = None,
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
    db_conn : MySQLdb.Connection
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


def get_charts_slugs(db_conn: Optional[MySQLdb.Connection] = None) -> pd.DataFrame:
    if db_conn is None:
        db_conn = get_connection()

    # Get a dataframe chart_id,char_slug, for all charts that have variables with an ETL path.
    query = """\
    SELECT
        c.id AS chart_id,
        c.slug AS chart_slug
    FROM charts c
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


def get_charts_views(db_conn: Optional[MySQLdb.Connection] = None) -> pd.DataFrame:
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

    # Create a column with the step name.
    # First assume all steps are public (hence starting with "data://").
    # Then edit private steps so they start with "data-private://".
    df["step"] = ["data://" + "/".join(etl_path.split("#")[0].split("/")[:-1]) for etl_path in df["etl_path"]]
    df.loc[df["is_private"], "step"] = df[df["is_private"]]["step"].str.replace("data://", "data-private://")

    return df


def read_sql(sql: str, engine: Optional[Engine] = None, *args, **kwargs) -> pd.DataFrame:
    """Wrapper around pd.read_sql that creates a connection and closes it after reading the data.
    This adds overhead, so if you need performance, reuse the same connection and cursor.
    """
    engine = engine or get_engine()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        with engine.connect() as con:
            return pd.read_sql(sql, con.connection, *args, **kwargs)

import re
import urllib.error
import urllib.parse

import pandas as pd
import requests

from etl.analytics.config import ANALYTICS_CSV_URL, MAX_DATASETTE_N_ROWS
from etl.analytics.utils import _safe_concat, clean_sql_query


class DatasetteSQLError(Exception):
    """Exception raised when a Datasette SQL query fails with a clear error message."""

    pass


def _get_datasette_error_from_json(sql_url: str) -> str:
    """Get error message from Datasette JSON endpoint."""
    # Convert CSV URL to JSON URL
    json_url = sql_url.replace(".csv?", ".json?")
    response = requests.get(json_url, timeout=10)

    # Try to parse JSON response regardless of status code
    try:
        data = response.json()
        if not data.get("ok", True):
            return data.get("error", "Unknown error")
    except (ValueError, KeyError):
        pass

    return f"HTTP {response.status_code}: {response.reason}"


def _try_to_execute_datasette_query(sql_url: str, warn: bool = False) -> pd.DataFrame:
    try:
        df = pd.read_csv(sql_url)
        return df
    except urllib.error.HTTPError as e:
        if e.code == 414:
            raise ValueError("HTTP 414: Query too long. Consider simplifying or batching the request.")
        else:
            # Get better error message from JSON endpoint
            error_msg = _get_datasette_error_from_json(sql_url)
            raise DatasetteSQLError(f"Datasette SQL Error: {error_msg}")
    except pd.errors.EmptyDataError:
        # Get better error message from JSON endpoint
        error_msg = _get_datasette_error_from_json(sql_url)
        raise DatasetteSQLError(f"Datasette SQL Error: {error_msg}")


def read_datasette(
    sql: str,
    datasette_csv_url: str = ANALYTICS_CSV_URL,
    chunk_size: int = MAX_DATASETTE_N_ROWS,
    use_https: bool = True,
) -> pd.DataFrame:
    """
    Execute a query in the Datasette semantic layer.

    Parameters
    ----------
    sql : str
        SQL query to execute.
    datasette_csv_url : str
        URL of the Datasette CSV endpoint.
    chunk_size : int
        Number of rows to fetch in each chunk.
        The default is 10,000 (which is the maximum number of rows that Datasette can return in a single request).
        If the query contains a LIMIT clause, it should be smaller than chunk_size (otherwise, an error is raised).
        If the query does not contain a LIMIT clause, the query is paginated using LIMIT and OFFSET.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the results of the query.

    """
    if use_https:
        datasette_csv_url = datasette_csv_url.replace("http://", "https://")

    # Check if the query uses SQLite DATE() function instead of ISO date literals.
    if re.search(r"\bDATE\s*\(", sql, re.IGNORECASE):
        raise DatasetteSQLError(
            "SQLite DATE() function is not supported in Datasette queries. "
            "Use ISO date literals instead. Example: DATE '2025-01-01' - INTERVAL '1 year'"
        )

    # Check if the query contains a LIMIT clause.
    limit_match = re.search(r"\bLIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?\b", sql, re.IGNORECASE)

    # Clean the query.
    sql_clean = clean_sql_query(sql)

    if limit_match:
        # If a LIMIT clause already exists, check if it's larger than the limit.
        limit_value = int(limit_match.group(1))
        if limit_value > MAX_DATASETTE_N_ROWS:
            raise DatasetteSQLError(
                f"Query LIMIT ({limit_value}) exceeds Datasette's maximum row limit ({MAX_DATASETTE_N_ROWS}). Either use a lower value for the limit, or set no limit (and pagination will be used)."
            )
        else:
            # Given that there is a LIMIT clause, and the value is small, execute the query as-is.
            full_url = f"{datasette_csv_url}?" + urllib.parse.urlencode({"sql": sql_clean, "_size": "max"})
            # Fetch data as a dataframe, or raise an error (e.g. if query is too long).
            df = _try_to_execute_datasette_query(sql_url=full_url, warn=True)
    else:
        # If there is no LIMIT clause, paginate using LIMIT/OFFSET.
        offset = 0
        dfs = []
        while True:
            # Prepare query for this chunk.
            full_url = f"{datasette_csv_url}?" + urllib.parse.urlencode(
                {"sql": f"{sql_clean} LIMIT {chunk_size} OFFSET {offset}", "_size": "max"}
            )
            # Fetch data for current chunk.
            df_chunk = _try_to_execute_datasette_query(sql_url=full_url)
            if len(df_chunk) == chunk_size:
                # Add data for current chunk to the list.
                dfs.append(df_chunk)
                # Update offset.
                offset += chunk_size
            else:
                # If fewer rows than the maximum (or even zero rows) are fetched, this must be the last chunk.
                dfs.append(df_chunk)
                break

        # Concatenate all chunks of data.
        df = _safe_concat(dfs)

    return df

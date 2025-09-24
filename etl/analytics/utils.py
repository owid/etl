"""Generic utils for analytics in ETL."""

from typing import List

import pandas as pd
from structlog import get_logger

# Initialize logger.
log = get_logger()


def _safe_concat(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate DataFrames, ignoring empty ones."""
    # Filter out empty DataFrames.
    dfs_to_concat = [df_ for df_ in dfs if not df_.empty]
    columns = list(dict.fromkeys(col for df_ in dfs for col in df_.columns))

    # Concatenate only if there are non-empty DataFrames
    if dfs_to_concat:
        df = pd.concat(dfs_to_concat, ignore_index=True)
    else:
        df = pd.DataFrame(columns=columns)

    return df


def clean_sql_query(sql: str) -> str:
    """
    Normalize an SQL string for use in Datasette URL queries.

    Parameters
    ----------
    sql : str
        SQL query to clean.
    Returns
    -------
    str
        Cleaned SQL query.

    """
    return " ".join(sql.strip().rstrip(";").split())

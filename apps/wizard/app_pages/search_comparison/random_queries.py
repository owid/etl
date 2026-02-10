"""Random search query sampler for testing search functionality.

Fetches search queries from BigQuery and provides weighted random sampling
to simulate realistic user searches.
"""

from datetime import date, timedelta

import pandas as pd

from etl.config import memory
from etl.google import read_gbq


@memory.cache
def fetch_search_queries(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch search query data from BigQuery (cached)."""
    query = f"""
        SELECT
            query,
            SUM(n_searches) as n_searches,
            MAX(n_hits) as n_hits
        FROM `owid-analytics.prod_algolia.search_metrics_by_day_index_query`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
          AND `index` = 'explorer-views-and-charts'
        GROUP BY query
        HAVING LENGTH(query) >= 3
    """
    return read_gbq(query, project_id="owid-analytics")


def clean_queries(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out junk queries (URLs, path-like strings)."""
    df = df[~df["query"].str.contains("ourworldindata", case=False, na=False)]
    df = df[~df["query"].str.startswith("/", na=False)]
    return df


def remove_prefix_queries(df: pd.DataFrame) -> pd.DataFrame:
    """Remove queries that are prefixes of more popular queries.

    E.g., if "tobacco" has 100 searches and "tobac" has 5, drop "tobac".
    Uses a trie for O(n * m) complexity where m is avg query length.
    """
    # Sort by search volume descending - higher volume queries take priority
    df = df.sort_values("n_searches", ascending=False).copy()
    queries_sorted = df["query"].tolist()

    # Build a simple trie to track existing queries
    # Each node is a dict with "_children" for child nodes and "_end" if a query ends here
    trie: dict = {"_children": {}}
    queries_to_keep = set()

    for query in queries_sorted:
        # Walk the trie to check if this query is a prefix of an existing one
        node = trie
        is_new_path = False

        for char in query:
            if char not in node["_children"]:
                is_new_path = True
                node["_children"][char] = {"_children": {}}
            node = node["_children"][char]

        # If we created new nodes OR this path has no children, it's not a prefix of existing
        has_children = bool(node["_children"])
        if is_new_path or not has_children:
            node["_end"] = True
            queries_to_keep.add(query)

    return pd.DataFrame(df[df["query"].isin(queries_to_keep)]).reset_index(drop=True)


def get_random_search_query(
    require_hits: bool = True,
    days: int = 7,
    filter_prefixes: bool = True,
) -> str:
    """Get a random search query weighted by search volume.

    Args:
        require_hits: If True, include all.
                     If False, only include queries with zero results.
        days: Number of days to look back for data.
        filter_prefixes: If True, remove queries that are prefixes of more popular queries.

    Returns:
        A random search query string.
    """
    # Calculate date range
    end_date = date.today() - timedelta(days=2)  # Algolia data has ~1 day lag
    start_date = end_date - timedelta(days=days)

    # Fetch data (cached)
    df = fetch_search_queries(start_date.isoformat(), end_date.isoformat())

    # Filter by hits
    if not require_hits:
        df = df[df["n_hits"] == 0]

    df = clean_queries(df)

    if len(df) == 0:
        return "climate change"  # Fallback

    # Remove prefix queries (e.g., "tobac" when "tobacco" exists)
    if filter_prefixes:
        df = remove_prefix_queries(df)

    if len(df) == 0:
        return "climate change"  # Fallback

    # Weighted random sampling by search volume
    weights = df["n_searches"].astype(float)
    weights = weights / weights.sum()

    return df.sample(n=1, weights=weights)["query"].iloc[0]

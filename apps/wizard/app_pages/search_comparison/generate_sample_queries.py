"""Generate a file of sample search queries from BigQuery.

Usage (from staging server or locally):
    python apps/wizard/app_pages/search_comparison/generate_sample_queries.py [--n 200] [--days 30] [--output sample_queries.txt]
"""

import argparse
from datetime import date, timedelta

import pandas as pd

from etl.google import read_gbq


def fetch_search_queries(start_date: str, end_date: str) -> pd.DataFrame:
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
    """Filter out junk queries."""
    df = df[~df["query"].str.contains("ourworldindata", case=False, na=False)]
    df = df[~df["query"].str.startswith("/", na=False)]
    return df


def remove_prefix_queries(df: pd.DataFrame) -> pd.DataFrame:
    """Remove queries that are prefixes of more popular queries."""
    df = df.sort_values("n_searches", ascending=False).copy()
    queries_sorted = df["query"].tolist()

    trie: dict = {"_children": {}}
    queries_to_keep = set()

    for q in queries_sorted:
        node = trie
        is_new_path = False
        for char in q:
            if char not in node["_children"]:
                is_new_path = True
                node["_children"][char] = {"_children": {}}
            node = node["_children"][char]

        if is_new_path or not bool(node["_children"]):
            node["_end"] = True
            queries_to_keep.add(q)

    return df[df["query"].isin(queries_to_keep)].reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser(description="Generate sample search queries from BigQuery")
    parser.add_argument("--n", type=int, default=200, help="Number of queries to sample (default: 200)")
    parser.add_argument("--days", type=int, default=30, help="Days to look back (default: 30)")
    parser.add_argument("--output", type=str, default="sample_queries.txt", help="Output file path")
    args = parser.parse_args()

    end_date = date.today() - timedelta(days=2)
    start_date = end_date - timedelta(days=args.days)

    print(f"Fetching queries from {start_date} to {end_date}...")
    df = fetch_search_queries(start_date.isoformat(), end_date.isoformat())
    print(f"  {len(df)} raw queries")

    df = clean_queries(df)
    print(f"  {len(df)} after cleaning")

    df = remove_prefix_queries(df)
    print(f"  {len(df)} after removing prefix queries")

    # Weighted sample without replacement
    n = min(args.n, len(df))
    weights = df["n_searches"].astype(float)
    weights = weights / weights.sum()
    sampled = df.sample(n=n, weights=weights, replace=False)

    # Sort alphabetically for readability
    queries = sorted(sampled["query"].tolist())

    with open(args.output, "w") as f:
        for q in queries:
            f.write(q + "\n")

    print(f"Wrote {len(queries)} queries to {args.output}")


if __name__ == "__main__":
    main()

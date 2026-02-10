"""Generate a file of sample search queries from BigQuery.

Usage (from staging server or locally):
    python apps/wizard/app_pages/search_comparison/generate_sample_queries.py [--n 1000] [--days 30] [--output sample_queries.txt]
"""

import argparse
from datetime import date, timedelta

from apps.wizard.app_pages.search_comparison.random_queries import (
    clean_queries,
    fetch_search_queries,
    remove_prefix_queries,
)


def main():
    parser = argparse.ArgumentParser(description="Generate sample search queries from BigQuery")
    parser.add_argument("--n", type=int, default=1000, help="Number of queries to sample (default: 1000)")
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

#
#  owid.catalog.api.datasette
#
#  Lightweight client for OWID's public Datasette instance.
#
from __future__ import annotations

from typing import Any, Literal

import pandas as pd
import requests

DATASETTE_BASE_URL = "https://datasette-public.owid.io/owid.json"


class DatasetteAPI:
    """Lightweight client for OWID's public Datasette instance.

    Provides SQL query execution and convenience methods for common operations.

    Example:
        ```python
        from owid.catalog.api import DatasetteAPI

        api = DatasetteAPI()

        # Execute raw SQL
        results = api.query("SELECT * FROM analytics_popularity LIMIT 10")

        # List available tables
        tables = api.list_tables()

        # Fetch popularity scores
        scores = api.fetch_popularity(["gdp-per-capita"], "indicator")
        ```
    """

    def __init__(self, base_url: str = DATASETTE_BASE_URL, timeout: int = 10) -> None:
        """Initialize the Datasette API client.

        Args:
            base_url: Datasette JSON API endpoint URL.
            timeout: HTTP request timeout in seconds.
        """
        self.base_url = base_url
        self.timeout = timeout

    def query(self, sql: str, timeout: int | None = None, paginate: bool = True) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame.

        Automatically paginates through all results using LIMIT/OFFSET.
        Datasette has a 1000-row limit per request for raw SQL queries.

        Args:
            sql: SQL query to execute. Should NOT include LIMIT/OFFSET if paginate=True.
            timeout: Optional timeout override for this request.
            paginate: If True (default), automatically fetch all pages of results.
                      If False, return only the first page (up to 1000 rows).

        Returns:
            DataFrame with query results. Empty DataFrame on error.
        """
        PAGE_SIZE = 1000
        request_timeout = timeout or self.timeout

        try:
            all_rows: list[dict[str, Any]] = []
            offset = 0

            while True:
                # Add LIMIT/OFFSET to the query for pagination
                paginated_sql = f"{sql.rstrip(';')} LIMIT {PAGE_SIZE} OFFSET {offset}"

                resp = requests.get(
                    self.base_url,
                    params={"sql": paginated_sql, "_shape": "array"},
                    timeout=request_timeout,
                )
                resp.raise_for_status()
                rows = resp.json()

                all_rows.extend(rows)

                # Stop if we got fewer rows than requested (last page) or not paginating
                if not paginate or len(rows) < PAGE_SIZE:
                    break

                offset += PAGE_SIZE

            return pd.DataFrame(all_rows)
        except Exception:
            return pd.DataFrame()

    def list_tables(self, timeout: int | None = None) -> list[str]:
        """List available tables in the database.

        Args:
            timeout: Optional timeout override for this request.

        Returns:
            List of table names. Empty list on error.
        """
        sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        df = self.query(sql, timeout=timeout)
        return df["name"].tolist() if not df.empty else []

    def fetch_popularity(
        self,
        slugs: list[str],
        type: Literal["indicator", "dataset"],
        timeout: int | None = None,
    ) -> dict[str, float]:
        """Fetch popularity scores for given slugs.

        Args:
            slugs: List of slugs to fetch popularity for.
            type: Type of popularity to fetch ("indicator" or "dataset").
            timeout: Optional timeout override for this request.

        Returns:
            Dict mapping slug to popularity score (0.0 to 1.0).
            Missing slugs will not be in the dict.
        """
        if not slugs:
            return {}

        # Escape single quotes in slugs
        escaped_slugs = [s.replace("'", "''") for s in slugs]
        slugs_str = ", ".join(f"'{s}'" for s in escaped_slugs)

        sql = f"""
        SELECT slug, popularity
        FROM analytics_popularity
        WHERE type = '{type}' AND slug IN ({slugs_str})
        """

        df = self.query(sql, timeout=timeout)
        if df.empty:
            return {}
        return dict(zip(df["slug"], df["popularity"].astype(float)))

    def __repr__(self) -> str:
        return f"DatasetteAPI(base_url={self.base_url!r})"

#
#  owid.catalog.api.datasette
#
#  Lightweight client for OWID's public Datasette instance.
#
from __future__ import annotations

from typing import Any, Literal

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

    def query(self, sql: str, timeout: int | None = None) -> list[dict[str, Any]]:
        """Execute a SQL query and return results as a list of dicts.

        Args:
            sql: SQL query to execute.
            timeout: Optional timeout override for this request.

        Returns:
            List of row dictionaries. Empty list on error.
        """
        try:
            resp = requests.get(
                self.base_url,
                params={"sql": sql, "_shape": "array"},
                timeout=timeout or self.timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return []

    def list_tables(self, timeout: int | None = None) -> list[str]:
        """List available tables in the database.

        Args:
            timeout: Optional timeout override for this request.

        Returns:
            List of table names. Empty list on error.
        """
        sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        results = self.query(sql, timeout=timeout)
        return [row["name"] for row in results]

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

        results = self.query(sql, timeout=timeout)
        return {row["slug"]: float(row["popularity"]) for row in results}

    def __repr__(self) -> str:
        return f"DatasetteAPI(base_url={self.base_url!r})"

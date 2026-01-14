#
#  owid.catalog.api.datasette
#
#  Lightweight client for OWID's public Datasette instance.
#
from __future__ import annotations

from typing import Any, Literal

import pandas as pd
import requests
from pydantic import BaseModel, ConfigDict, Field

DATASETTE_BASE_URL = "https://datasette-public.owid.io/owid.json"
DEFAULT_TIMEOUT = 10


class DatasetteTable(BaseModel):
    """Metadata for a Datasette table."""

    model_config = ConfigDict(frozen=True)

    name: str
    columns: list[str] = Field(default_factory=list)
    primary_keys: list[str] = Field(default_factory=list)
    row_count: int | None = None
    is_view: bool = False
    description: str | None = None

    def fetch(self, timeout: int = DEFAULT_TIMEOUT) -> DatasetteTable:
        """Fetch full metadata for this table.

        Returns a new DatasetteTable with all metadata fields populated.
        Useful when you have a table from list_tables() (fast mode) and want full details.

        Args:
            timeout: HTTP request timeout in seconds.

        Returns:
            DatasetteTable with full metadata (columns, row_count, etc.)

        Raises:
            requests.HTTPError: If the table doesn't exist or request fails.
        """
        data = _fetch_table_metadata(self.name, timeout=timeout)
        return _build_table_from_metadata(self.name, data)


class DatasetteAPI:
    """Lightweight client for OWID's public Datasette instance.

    Provides SQL query execution and convenience methods for common operations.

    Example:
        ```python
        from owid.catalog.api.datasette import DatasetteAPI

        api = DatasetteAPI()

        # Execute raw SQL
        results = api.query("SELECT * FROM analytics_popularity LIMIT 10")

        # List available tables
        tables = api.list_tables()

        # Fetch popularity scores
        scores = api.fetch_popularity(["gdp-per-capita"], "indicator")
        ```
    """

    def __init__(self, base_url: str = DATASETTE_BASE_URL, timeout: int = DEFAULT_TIMEOUT) -> None:
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

    def list_tables(self, with_metadata: bool = False, timeout: int | None = None) -> list[DatasetteTable]:
        """List available tables in the database.

        Args:
            with_metadata: If True, fetch full metadata for each table (slower, ~50 requests).
                          If False (default), return tables with just names (fast, 1 request).
            timeout: Optional timeout override for this request.

        Returns:
            List of DatasetteTable objects.
        """
        sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        df = self.query(sql, timeout=timeout)

        if df.empty:
            return []

        table_names = df["name"].tolist()

        if not with_metadata:
            # Fast path: just return tables with names only
            return [DatasetteTable(name=name) for name in table_names]

        # Slow path: fetch full metadata for each table
        request_timeout = timeout or self.timeout
        tables = []
        for name in table_names:
            try:
                tables.append(self.get_table(name, timeout=request_timeout))
            except Exception:
                # Fall back to name-only if metadata fetch fails
                tables.append(DatasetteTable(name=name))
        return tables

    def get_table(self, name: str, timeout: int | None = None) -> DatasetteTable:
        """Get full metadata for a single table.

        Args:
            name: Name of the table.
            timeout: Optional timeout override for this request.

        Returns:
            DatasetteTable with full metadata.

        Raises:
            requests.HTTPError: If the table doesn't exist or request fails.
        """
        request_timeout = timeout or self.timeout
        data = _fetch_table_metadata(name, base_url=self.base_url, timeout=request_timeout)
        return _build_table_from_metadata(name, data)

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


def _fetch_table_metadata(
    table_name: str,
    base_url: str = DATASETTE_BASE_URL,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Fetch metadata for a table from Datasette table endpoint.

    Args:
        table_name: Name of the table.
        base_url: Datasette JSON API endpoint URL.
        timeout: HTTP request timeout in seconds.

    Returns:
        Raw metadata dict from Datasette API.
    """
    # Build table endpoint URL from base URL
    # e.g., https://datasette-public.owid.io/owid.json -> https://datasette-public.owid.io/owid/{table}.json
    table_url = base_url.replace(".json", f"/{table_name}.json")
    resp = requests.get(
        table_url,
        params={"_size": 0},  # No rows, just metadata
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


def _build_table_from_metadata(name: str, data: dict[str, Any]) -> DatasetteTable:
    """Build a DatasetteTable from raw metadata dict."""
    return DatasetteTable(
        name=name,
        columns=data.get("columns", []),
        primary_keys=data.get("primary_keys", []),
        row_count=data.get("filtered_table_rows_count"),
        is_view=data.get("is_view", False),
        description=data.get("human_description_en") or None,
    )

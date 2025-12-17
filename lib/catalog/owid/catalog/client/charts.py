#
#  owid.catalog.client.charts
#
#  Charts API for fetching data from published OWID charts.
#
from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING, Any

import pandas as pd
import requests

from .models import ChartResult

if TYPE_CHECKING:
    from . import Client


class ChartNotFoundError(Exception):
    """Raised when a chart does not exist."""

    pass


class LicenseError(Exception):
    """Raised when chart data cannot be downloaded due to licensing."""

    pass


class ChartsAPI:
    """API for accessing OWID chart data and metadata.

    Provides methods to fetch data, metadata, and configuration from
    published charts on ourworldindata.org.

    Example:
        ```python
        from owid.catalog.client import Client

        client = Client()

        # Get chart data as DataFrame
        df = client.charts.get("life-expectancy")

        # Get chart metadata
        meta = client.charts.metadata("life-expectancy")

        # Get full chart info as ChartResult
        chart = client.charts.info("life-expectancy")
        df = chart.get_data()
        ```
    """

    BASE_URL = "https://ourworldindata.org/grapher"

    def __init__(self, client: "Client") -> None:
        self._client = client

    def get_data(self, slug_or_url: str) -> pd.DataFrame:
        """Fetch chart data as a DataFrame.

        Args:
            slug_or_url: Chart slug (e.g., "life-expectancy") or full URL.

        Returns:
            DataFrame with chart data. Additional metadata in df.attrs.

        Raises:
            ChartNotFoundError: If the chart does not exist.
            LicenseError: If the data cannot be downloaded due to licensing.

        Example:
            ```python
            df = client.charts.get_data("life-expectancy")
            print(df.head())
            ```
        """
        slug = self._parse_slug(slug_or_url)
        return self._fetch_data(slug)

    def metadata(self, slug_or_url: str) -> dict[str, Any]:
        """Fetch chart metadata.

        Args:
            slug_or_url: Chart slug or full URL.

        Returns:
            Dict containing chart metadata including column information.

        Example:
            ```python
            meta = client.charts.metadata("life-expectancy")
            print(meta["columns"].keys())
            ```
        """
        slug = self._parse_slug(slug_or_url)
        return self._fetch_metadata(slug)

    def config(self, slug_or_url: str) -> dict[str, Any]:
        """Fetch raw grapher configuration.

        Args:
            slug_or_url: Chart slug or full URL.

        Returns:
            Dict containing the grapher configuration.

        Example:
            ```python
            config = client.charts.config("life-expectancy")
            print(config["title"])
            ```
        """
        slug = self._parse_slug(slug_or_url)
        return self._fetch_config(slug)

    def fetch(self, slug_or_url: str) -> ChartResult:
        """Fetch a chart with all its metadata and config.

        Args:
            slug_or_url: Chart slug or full URL.

        Returns:
            ChartResult with metadata, config, and get_data() method.

        Example:
            ```python
            chart = client.charts.fetch("life-expectancy")
            print(chart.title)
            df = chart.get_data()
            ```
        """
        slug = self._parse_slug(slug_or_url)
        config = self._fetch_config(slug)
        metadata = self._fetch_metadata(slug)

        return ChartResult(
            slug=slug,
            title=config.get("title", ""),
            url=f"{self.BASE_URL}/{slug}",
            config=config,
            metadata=metadata,
        )

    @staticmethod
    def _parse_slug(slug_or_url: str) -> str:
        """Extract slug from URL or return as-is."""
        if slug_or_url.startswith("https://ourworldindata.org/grapher/"):
            # Handle URLs with query params
            path = slug_or_url.split("/grapher/")[1]
            return path.split("?")[0]
        elif slug_or_url.startswith("https://"):
            raise ValueError("URL must be a Grapher URL, e.g. https://ourworldindata.org/grapher/life-expectancy")
        return slug_or_url

    @staticmethod
    def _fetch_data(slug: str) -> pd.DataFrame:
        """Fetch CSV data from a chart."""
        url = f"{ChartsAPI.BASE_URL}/{slug}.csv?useColumnShortNames=true"
        resp = requests.get(url)

        if resp.status_code == 404:
            raise ChartNotFoundError(f"No such chart found: {slug}")

        if resp.status_code == 403:
            try:
                error_data = resp.json()
                raise LicenseError(error_data.get("error", "This chart contains non-redistributable data"))
            except (json.JSONDecodeError, ValueError):
                raise LicenseError("This chart contains non-redistributable data that cannot be downloaded")

        resp.raise_for_status()

        df = pd.read_csv(io.StringIO(resp.text))

        # Normalize column names
        df = df.rename(columns={"Entity": "entities", "Year": "years", "Day": "years"})
        if "Code" in df.columns:
            df = df.drop(columns=["Code"])

        # Attach metadata
        df.attrs["slug"] = slug
        df.attrs["url"] = f"{ChartsAPI.BASE_URL}/{slug}"

        # Rename "years" to "dates" if values are date strings
        if "years" in df.columns and df["years"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$").all():
            df = df.rename(columns={"years": "dates"})

        return df

    @staticmethod
    def _fetch_metadata(slug: str) -> dict[str, Any]:
        """Fetch metadata JSON from a chart."""
        url = f"{ChartsAPI.BASE_URL}/{slug}.metadata.json"
        resp = requests.get(url)

        if resp.status_code == 404:
            raise ChartNotFoundError(f"No such chart found: {slug}")

        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _fetch_config(slug: str) -> dict[str, Any]:
        """Fetch config JSON from a chart."""
        url = f"{ChartsAPI.BASE_URL}/{slug}.config.json"
        resp = requests.get(url)

        if resp.status_code == 404:
            raise ChartNotFoundError(f"No such chart found: {slug}")

        resp.raise_for_status()
        return resp.json()

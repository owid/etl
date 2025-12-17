#
#  owid.catalog.charts
#
#
#  Access to data in OWID charts.
#
#  DEPRECATED: This module is maintained for backwards compatibility.
#  New code should use owid.catalog.client.Client instead:
#
#    from owid.catalog import Client
#    client = Client()
#    df = client.charts.get_data("life-expectancy")
#

from dataclasses import dataclass

import pandas as pd

from .client import ChartNotFoundError, Client, LicenseError  # noqa


@dataclass
class Chart:
    """
    A chart published on Our World in Data, for example:

    https://ourworldindata.org/grapher/life-expectancy

    DEPRECATED: Use owid.catalog.client.Client instead.
    """

    slug: str

    _client: Client | None = None
    _chart_result: object = None  # ChartResult from client (delayed import)
    _metadata: dict | None = None

    def _get_client(self) -> Client:
        """Lazy initialization of the client."""
        if self._client is None:
            self._client = Client()
        return self._client

    def _fetch_chart(self):
        """Fetch chart data and metadata."""
        if self._chart_result is None:
            self._chart_result = self._get_client().charts.fetch(self.slug)

    @property
    def bundle(self):
        """
        DEPRECATED: For backwards compatibility only.
        Raises ChartNotFoundError if the chart does not exist.
        """
        # This property exists only to trigger the fetch and raise an error if the chart doesn't exist
        self._fetch_chart()
        return self

    @property
    def config(self) -> dict:
        """Get chart configuration."""
        self._fetch_chart()
        return self._chart_result.config  # type: ignore

    def get_data(self) -> pd.DataFrame:
        """
        Fetch chart data as a DataFrame.

        This method maintains backwards compatibility with the old API,
        including special column renaming for single-value charts.
        """
        # Fetch using the new client
        df = self._get_client().charts.get_data(self.slug)

        # Fetch metadata separately to add to attrs
        if self._metadata is None:
            self._metadata = self._get_client().charts.metadata(self.slug)

        # Apply old API transformations for backwards compatibility
        df = _apply_legacy_transforms(df, self.slug, self._metadata)

        return df

    def __lt__(self, other):
        return self.slug < other.slug

    def __eq__(self, value: object) -> bool:
        return isinstance(value, Chart) and value.slug == self.slug


def get_data(slug_or_url: str) -> pd.DataFrame:
    """
    Fetch the data for a chart by its slug or by the URL of the chart.

    Additional metadata about the chart is available in the DataFrame's `attrs` attribute.

    DEPRECATED: Use owid.catalog.client.Client instead:
        from owid.catalog import Client
        client = Client()
        df = client.charts.get_data("life-expectancy")
    """
    # Parse the slug from URL if needed
    if slug_or_url.startswith("https://ourworldindata.org/grapher/"):
        slug = slug_or_url.split("/grapher/")[-1].split("?")[0]
    elif slug_or_url.startswith("https://"):
        raise ValueError("URL must be a Grapher URL, e.g. https://ourworldindata.org/grapher/life-expectancy")
    else:
        slug = slug_or_url

    return Chart(slug).get_data()


def _apply_legacy_transforms(df: pd.DataFrame, slug: str, metadata: dict) -> pd.DataFrame:
    """
    Apply legacy transformations to maintain backwards compatibility with old API.

    The old API had special behavior:
    - Single value columns were renamed to the slug (with underscores)
    - Metadata was stored differently in attrs
    """
    # Build per-column metadata from the metadata.json response
    columns_meta = metadata.get("columns", {})
    df.attrs["metadata"] = {}
    for col_name, col_meta in columns_meta.items():
        short_name = col_meta.get("shortName", col_name)
        if short_name in df.columns:
            df.attrs["metadata"][short_name] = col_meta

    # If there's only one data column, rename it to the slug-based name
    value_cols = [c for c in df.columns if c not in ("entities", "years", "dates")]
    if len(value_cols) == 1:
        old_name = value_cols[0]
        new_name = slug.replace("-", "_")
        df = df.rename(columns={old_name: new_name})

        # Update metadata if present
        if old_name in df.attrs["metadata"]:
            df.attrs["metadata"][new_name] = df.attrs["metadata"].pop(old_name)

        df.attrs["value_col"] = new_name

    return df

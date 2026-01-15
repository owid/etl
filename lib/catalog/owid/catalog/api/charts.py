#
#  owid.catalog.api.charts
#
#  Charts API for fetching data from published OWID charts.
#
from __future__ import annotations

import io
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pandas as pd
import requests
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from owid.catalog.api.models import ResponseSet
from owid.catalog.api.utils import _loading_data_from_api
from owid.catalog.core.charts import ChartTable, ChartTableMeta
from owid.catalog.meta import Origin, VariableMeta

if TYPE_CHECKING:
    from owid.catalog.api import Client


# =============================================================================
# Exceptions
# =============================================================================


class ChartNotFoundError(Exception):
    """Raised when a chart does not exist."""

    pass


class LicenseError(Exception):
    """Raised when chart data cannot be downloaded due to licensing."""

    pass


# =============================================================================
# Chart Loading Functions
# =============================================================================


def _load_chart_table(
    slug: str,
    *,
    base_url: str,
    use_column_short_names: bool,
    load_data: bool = True,
    timeout: int = 30,
    base_url: str = GRAPHER_BASE_URL,
) -> ChartTable:
    """Load chart data as ChartTable with rich metadata.

    Args:
        slug: Chart slug (e.g., "life-expectancy").
        base_url: Base URL for the Grapher (required).
        use_column_short_names: If True, use short column names (e.g., "life_expectancy_0"). If False, use full display names (e.g., "Life expectancy at birth").
        load_data: If True (default), load full chart data.
                   If False, load only structure (columns and metadata) without rows.
        timeout: HTTP request timeout in seconds.
        base_url: Base URL for the Grapher. Defaults to GRAPHER_BASE_URL.

    Returns:
        ChartTable with chart data and chart_config. Column metadata (unit, description, etc.)
        is populated from the chart's metadata.json.

    Raises:
        ChartNotFoundError: If the chart does not exist.
        LicenseError: If the chart contains non-redistributable data.
    """
    message = f"Fetching chart '{slug}'"

    def _load():
        # Fetch metadata (contains column info)
        metadata = _load_chart_table_metadata(slug, timeout=timeout, base_url=base_url)

        # Build lookup from shortName to column metadata
        columns_meta = metadata.get("columns", {})
        short_name_lookup = {col_info.get("shortName"): col_info for col_info in columns_meta.values()}

        # Fetch config
        config = _load_chart_table_config(slug, timeout=timeout, base_url=base_url)

        # Load data from CSV as ChartTable
        df = _load_chart_table_data(
            slug,
            timeout=timeout,
            load_data=load_data,
            base_url=base_url,
            params={
                "useColumnShortNames": str(use_column_short_names).lower(),
            },
        )

        # Build table with metadata
        meta = ChartTableMeta(
            short_name=slug,
            title=config.get("title"),
            description=config.get("subtitle"),
            chart_config=config,
        )
        tb = ChartTable(df, metadata=meta)

        # Apply column metadata to data columns
        for col in tb.columns:
            if col in ("entities", "years", "dates"):
                continue
            col_info = short_name_lookup.get(col, {})
            if col_info:
                tb[col].metadata = _build_variable_meta(col_info)

        # Set index columns (entities + years/dates)
        index_cols = []
        if "entities" in tb.columns:
            index_cols.append("entities")
        if "years" in tb.columns:
            index_cols.append("years")
        elif "dates" in tb.columns:
            index_cols.append("dates")
        if index_cols:
            # Preserve column metadata before set_index
            col_metadata = {col: tb[col].metadata for col in tb.columns if col not in index_cols}

            # set_index may return Table instead of ChartTable due to pandas subclass behavior
            # Convert back to ChartTable, preserving metadata with 'like' parameter
            indexed = tb.set_index(index_cols)
            tb = ChartTable(indexed, like=indexed)

            # Restore column metadata
            for col, meta in col_metadata.items():
                if col in tb.columns:
                    tb[col].metadata = meta

        return tb

    if load_data:
        with _loading_data_from_api(message):
            return _load()
    else:
        return _load()


def parse_chart_slug(slug_or_url: str) -> str:
    """Extract slug from URL or return as-is.

    Args:
        slug_or_url: Chart slug or full Grapher URL.

    Returns:
        Chart slug.

    Raises:
        ValueError: If URL is not a valid Grapher URL.
    """
    if slug_or_url.startswith("https://ourworldindata.org/grapher/"):
        # Handle URLs with query params
        path = slug_or_url.split("/grapher/")[1]
        return path.split("?")[0]
    elif slug_or_url.startswith("https://"):
        raise ValueError("URL must be a Grapher URL, e.g. https://ourworldindata.org/grapher/life-expectancy")
    return slug_or_url


def _load_chart_table_metadata(slug: str, *, timeout: int, base_url: str) -> dict[str, Any]:
    """Fetch metadata JSON from a chart.

    Args:
        slug: Chart slug.
        timeout: HTTP request timeout in seconds.
        base_url: Base URL for the Grapher (required).

    Returns:
        Metadata dictionary containing column info.

    Raises:
        ChartNotFoundError: If the chart does not exist.
    """
    url = f"{base_url}/{slug}.metadata.json"
    resp = requests.get(url, timeout=timeout)

    if resp.status_code == 404:
        raise ChartNotFoundError(f"Failed to retrieve chart metadata. No such chart found: {slug}")

    resp.raise_for_status()
    return resp.json()


def _load_chart_table_config(slug: str, *, timeout: int, base_url: str) -> dict[str, Any]:
    """Fetch config JSON from a chart.

    Args:
        slug: Chart slug.
        timeout: HTTP request timeout in seconds.
        base_url: Base URL for the Grapher (required).

    Returns:
        Chart configuration dictionary.

    Raises:
        ChartNotFoundError: If the chart does not exist.
    """
    url = f"{base_url}/{slug}.config.json"
    resp = requests.get(url, timeout=timeout)

    if resp.status_code == 404:
        raise ChartNotFoundError(f"Failed to retrieve chart config. No such chart found: {slug}")

    resp.raise_for_status()
    return resp.json()


def _load_chart_table_data(
    slug: str,
    *,
    timeout: int,
    params: dict[str, str],
    base_url: str,
    load_data: bool = True,
    base_url: str = GRAPHER_BASE_URL,
) -> pd.DataFrame:
    """Fetch chart data as a ChartTable.

    Args:
        slug: Chart slug.
        timeout: HTTP request timeout in seconds.
        params: Query parameters for the CSV endpoint. Refer to https://docs.owid.io/projects/etl/api/chart-api/#get-grapherslugcsv for complete list of parameters available.
        base_url: Base URL for the Grapher (required).
        load_data: If True, load full data. If False, load only header.

    Returns:
        ChartTable with data (or empty if load_data=False).

    Raises:
        ChartNotFoundError: If the chart does not exist.
        LicenseError: If the chart contains non-redistributable data.
    """
    url = f"{base_url}/{slug}.csv"
    resp = requests.get(
        url,
        params=params,
        timeout=timeout,
    )

    if resp.status_code == 404:
        raise ChartNotFoundError(f"Failed to retrieve chart data. No such chart found: {slug}")

    if resp.status_code == 403:
        try:
            error_data = resp.json()
            raise LicenseError(error_data.get("error", "This chart contains non-redistributable data"))
        except (ValueError, requests.exceptions.JSONDecodeError):
            raise LicenseError("This chart contains non-redistributable data that cannot be downloaded")

    resp.raise_for_status()

    if load_data:
        df = pd.read_csv(io.StringIO(resp.text))
    else:
        # Load only header (first row)
        df = pd.read_csv(io.StringIO(resp.text), nrows=0)

    # Normalize column names
    df = df.rename(columns={"Entity": "entities", "Year": "years", "Day": "years"})
    if "Code" in df.columns:
        df = df.drop(columns=["Code"])

    # Rename "years" to "dates" if values are date strings
    if load_data and "years" in df.columns and len(df) > 0:
        if df["years"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$").all():
            df = df.rename(columns={"years": "dates"})

    return df


def _build_variable_meta(col_info: dict[str, Any]) -> VariableMeta:
    """Build VariableMeta from chart column metadata.

    Args:
        col_info: Column metadata dictionary from chart metadata.json.

    Returns:
        VariableMeta with populated fields.
    """
    # Build Origin with citation info
    origins = []
    citation_full = col_info.get("citationLong")
    if citation_full:
        # Extract producer from citationShort (format: "Producer - Year")
        citation_short = col_info.get("citationShort", "")
        producer = citation_short.split(" – ")[0] if citation_short else ""

        origin = Origin(
            producer=producer,
            title=col_info.get("titleLong") or "",
            citation_full=citation_full,
            date_published=col_info.get("lastUpdated"),
        )
        origins.append(origin)

    return VariableMeta(
        title=col_info.get("titleShort"),
        description_short=col_info.get("descriptionShort"),
        description_key=col_info.get("descriptionKey", []),
        description_processing=col_info.get("descriptionProcessing"),
        unit=col_info.get("unit"),
        short_unit=col_info.get("shortUnit"),
        origins=origins,
    )


# =============================================================================
# ChartResult Model
# =============================================================================


class ChartResult(BaseModel):
    """An OWID chart (from fetch or search).

    Fields populated depend on the source:
    - fetch(): Provides config and metadata
    - search(): Provides subtitle, available_entities, num_related_articles, published_at, last_updated, popularity

    Core fields (slug, title, url) are always populated.

    Attributes:
        slug: Chart URL identifier (e.g., "life-expectancy").
        title: Chart title.
        url: Full URL to the interactive chart.
        config: Raw grapher configuration dict (from fetch).
        metadata: Chart metadata dict including column info (from fetch).
        subtitle: Chart subtitle/description (from search).
        available_entities: List of entities/countries in the chart (from search).
        num_related_articles: Number of related articles (from search).
        published_at: When the chart was first published (from search).
        last_updated: When the chart was last updated (from search).
        popularity: Popularity score (0.0 to 1.0) based on analytics views (from search).
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    # Core fields (always present)
    slug: str
    title: str
    url: str

    # From fetch() - full chart details
    config: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    # From search() - search-specific metadata
    subtitle: str = ""
    available_entities: list[str] = Field(default_factory=list)
    num_related_articles: int = 0
    published_at: datetime | None = None
    last_updated: datetime | None = None

    # Usage metadata
    popularity: float = 0.0

    # API configuration (immutable)
    base_url: str = Field(frozen=True)

    # Private cached data field
    _cached_chart_table: ChartTable | None = PrivateAttr(default=None)
    _timeout: int = PrivateAttr(default=30)

    def fetch(
        self,
        *,
        load_data: bool = True,
    ) -> ChartTable:
        """Fetch chart data as ChartTable with rich metadata.

        Args:
            load_data: If True (default), load full chart data.
                       If False, load only structure (columns and metadata) without rows.

        Returns:
            ChartTable with chart data and chart_config. Column metadata (unit, description, etc.)
            is populated from the chart's metadata.json.

        Example:
            ```python
            result = client.charts.search("life expectancy")[0]
            tb = result.fetch()
            print(tb.head())
            print(tb["life_expectancy_0"].metadata.unit)
            ```
        """
        # Return cached if available and requesting full data
        if load_data and self._cached_chart_table is not None:
            return self._cached_chart_table

        tb = _load_chart_table(
            self.slug,
            load_data=load_data,
            timeout=self._timeout,
            use_column_short_names=True,
            base_url=self.base_url,
        )

        # Cache only if loading full data with default params
        if load_data:
            self._cached_chart_table = tb

        return tb


# =============================================================================
# ChartsAPI
# =============================================================================


class ChartsAPI:
    """API for accessing OWID chart data and metadata.

    Provides methods to fetch data and metadata from published charts
    on ourworldindata.org. Also includes search functionality to find
    charts by keywords.

    Example:
        ```python
        from owid.catalog import Client

        client = Client()

        # Fetch chart data as ChartTable
        tb = client.charts.fetch("life-expectancy")
        print(tb.head())
        print(tb["life_expectancy_0"].metadata.unit)
        print(tb.metadata.chart_config.get("title"))  # Access chart config

        # Search for charts
        results = client.charts.search("gdp per capita")
        tb = results[0].fetch()  # Fetch chart data as ChartTable
        ```
    """

    def __init__(self, client: "Client", base_url: str) -> None:
        """Initialize the ChartsAPI.

        Args:
            client: The Client instance.
            base_url: Base URL for the Grapher (e.g., "https://ourworldindata.org/grapher").
        """
        self._client = client
        self._base_url = base_url

    @property
    def base_url(self) -> str:
        """Base URL for the Grapher (read-only)."""
        return self._base_url

    def search(
        self,
        query: str,
        *,
        countries: list[str] | None = None,
        topics: list[str] | None = None,
        require_all_countries: bool = False,
        limit: int = 20,
        page: int = 0,
        timeout: int | None = None,
    ) -> ResponseSet[ChartResult]:
        """Search for charts matching a query.

        Args:
            query: Search query string.
            countries: Optional list of country names to filter by.
            topics: Optional list of topic names to filter by.
            require_all_countries: If True, only return charts with ALL
                specified countries. Default False (any country matches).
            limit: Maximum results to return (1-100). Default 20.
            page: Page number for pagination (0-indexed). Default 0.
            timeout: HTTP request timeout in seconds. Defaults to client timeout.

        Returns:
            ResponseSet containing ChartResult objects, sorted by popularity (most viewed first).
            Each result includes a `popularity` field (0.0-1.0) based on analytics views.

        Example:
            ```python
            # Basic search (sorted by popularity)
            results = client.charts.search("life expectancy")
            for chart in results:
                print(f"{chart.title}: popularity={chart.popularity:.3f}")

            # Filter by countries
            results = client.charts.search(
                "gdp",
                countries=["France", "Germany"],
                require_all_countries=True
            )

            # Get data from search results
            tb = results[0].fetch()
            ```
        """
        return self._client._site_search.charts(
            query=query,
            countries=countries,
            topics=topics,
            require_all_countries=require_all_countries,
            limit=limit,
            page=page,
            timeout=timeout or self._client.timeout,
            grapher_url=self.base_url,
        )

    def fetch(
        self,
        slug_or_url: str,
        *,
        load_data: bool = True,
        timeout: int | None = None,
    ) -> ChartTable:
        """Fetch chart data as a ChartTable with rich metadata.

        Args:
            slug_or_url: Chart slug or full URL.
            load_data: If True (default), load full chart data.
                       If False, load only structure (columns and metadata) without rows.
            timeout: HTTP request timeout in seconds. Defaults to client timeout.

        Returns:
            ChartTable with chart data and chart_config. Column metadata (unit, description, etc.)
            is populated from the chart's metadata.json. Chart config is accessible via .metadata.chart_config.

        Example:
            ```python
            tb = client.charts.fetch("life-expectancy")
            print(tb.head())
            print(tb["life_expectancy_0"].metadata.unit)
            print(tb.metadata.chart_config.get("title"))
            ```
        """
        effective_timeout = timeout or self._client.timeout
        slug = parse_chart_slug(slug_or_url)
        return _load_chart_table(
            slug,
            load_data=load_data,
            timeout=effective_timeout,
            use_column_short_names=True,
            base_url=self.base_url,
        )

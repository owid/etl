#
#  owid.catalog.api.charts
#
#  Charts API for fetching data from published OWID charts.
#
from __future__ import annotations

import io
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, NamedTuple

import pandas as pd
import requests
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from owid.catalog.api.models import ResponseSet
from owid.catalog.api.utils import _loading_data_from_api
from owid.catalog.core.charts import ChartTable, ChartTableMeta
from owid.catalog.core.meta import Origin, VariableMeta

if TYPE_CHECKING:
    from owid.catalog.api import Client


# =============================================================================
# Constants
# =============================================================================

#: Chart type for standard grapher charts.
CHART: Literal["chart"] = "chart"
#: Chart type for explorer views.
EXPLORER: Literal["explorerView"] = "explorerView"
#: Chart type for multi-dimensional views.
MULTI_DIM: Literal["multiDimView"] = "multiDimView"

#: Union type for all chart types.
ChartType = Literal["chart", "explorerView", "multiDimView"]


# =============================================================================
# Exceptions
# =============================================================================


class ChartNotFoundError(Exception):
    """Raised when a chart does not exist."""

    pass


class LicenseError(Exception):
    """Raised when chart data cannot be downloaded due to licensing."""

    pass


class ExplorerFetchError(Exception):
    """Raised when an explorer view cannot be fetched (e.g., 503 from CSV endpoint)."""

    pass


# =============================================================================
# Chart Loading Functions
# =============================================================================


def _parse_query_params(query_params: str) -> dict[str, str]:
    """Parse a query parameter string into a dict.

    Args:
        query_params: Query string (e.g., "?Metric=Excess+mortality&Tab=Chart" or "Metric=Excess+mortality").

    Returns:
        Dict of parameter key-value pairs.
    """
    from urllib.parse import parse_qs

    # Strip leading "?"
    qs = query_params.lstrip("?")
    parsed = parse_qs(qs, keep_blank_values=True)
    # parse_qs returns lists; take first value
    return {k: v[0] for k, v in parsed.items()}


def _load_chart_table(
    slug: str,
    *,
    base_url: str,
    use_column_short_names: bool,
    load_data: bool = True,
    timeout: int = 30,
    extra_params: dict[str, str] | None = None,
) -> ChartTable:
    """Load chart data as ChartTable with rich metadata.

    Args:
        slug: Chart slug (e.g., "life-expectancy").
        base_url: Base URL for the Grapher (required).
        use_column_short_names: If True, use short column names (e.g., "life_expectancy_0"). If False, use full display names (e.g., "Life expectancy at birth").
        load_data: If True (default), load full chart data.
                   If False, load only structure (columns and metadata) without rows.
        timeout: HTTP request timeout in seconds.
        extra_params: Additional query parameters to pass to the CSV endpoint (e.g., for explorer views).

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
        csv_params: dict[str, str] = {
            "useColumnShortNames": str(use_column_short_names).lower(),
        }
        if extra_params:
            csv_params.update(extra_params)

        df = _load_chart_table_data(
            slug,
            timeout=timeout,
            load_data=load_data,
            base_url=base_url,
            params=csv_params,
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


class ParsedSlug(NamedTuple):
    """Result of parsing a chart slug or URL."""

    slug: str
    query_params: str
    type: Literal["chart", "explorerView"]


def parse_chart_slug(slug_or_url: str) -> ParsedSlug:
    """Extract slug, query params, and type from a URL or plain slug.

    Args:
        slug_or_url: Chart slug, grapher URL, or explorer URL.

    Returns:
        ParsedSlug with slug, query_params, and type.

    Raises:
        ValueError: If URL is not a valid grapher or explorer URL.
    """
    if slug_or_url.startswith("https://ourworldindata.org/grapher/"):
        path = slug_or_url.split("/grapher/")[1]
        slug = path.split("?")[0]
        query_params = "?" + path.split("?", 1)[1] if "?" in path else ""
        return ParsedSlug(slug=slug, query_params=query_params, type=CHART)
    elif slug_or_url.startswith("https://ourworldindata.org/explorers/"):
        path = slug_or_url.split("/explorers/")[1]
        slug = path.split("?")[0]
        query_params = "?" + path.split("?", 1)[1] if "?" in path else ""
        return ParsedSlug(slug=slug, query_params=query_params, type=EXPLORER)
    elif slug_or_url.startswith("https://"):
        raise ValueError(
            "URL must be a grapher or explorer URL, e.g.\n"
            "  https://ourworldindata.org/grapher/life-expectancy\n"
            "  https://ourworldindata.org/explorers/covid?Metric=Cases"
        )
    # Plain slug, possibly with query params (e.g. "education-spending?level=primary")
    if "?" in slug_or_url:
        slug, params = slug_or_url.split("?", 1)
        return ParsedSlug(slug=slug, query_params="?" + params, type=CHART)
    return ParsedSlug(slug=slug_or_url, query_params="", type=CHART)


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
        raise ChartNotFoundError(f"Failed to retrieve chart metadata. No such chart found: {slug} (url {url})")

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

    # Normalize column names (handle both old capitalized and new lowercase API responses)
    df = df.rename(
        columns={
            "Entity": "entities",
            "entity": "entities",
            "Year": "years",
            "year": "years",
            "Day": "years",
            "day": "years",
        }
    )
    if "Code" in df.columns:
        df = df.drop(columns=["Code"])
    if "code" in df.columns:
        df = df.drop(columns=["code"])

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
    type: ChartType = CHART

    # Query parameters (for explorer views, e.g., "?Metric=Excess+mortality+...")
    query_params: str = ""

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
    site_url: str = Field(frozen=True)

    @property
    def chart_base_url(self) -> str:
        """Base URL for this chart type (grapher or explorer, derived from site_url and type)."""
        if self.type == EXPLORER:
            return f"{self.site_url}/explorers"
        return f"{self.site_url}/grapher"

    @property
    def url(self) -> str:
        """Full URL to the interactive chart (built from chart_base_url, slug, and query_params)."""
        if self.query_params:
            return f"{self.chart_base_url}/{self.slug}{self.query_params}"
        return f"{self.chart_base_url}/{self.slug}"

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

        Note:
            Explorer views (``type="explorerView"``) are best-effort. Some explorers
            may return 503 or other errors from their CSV endpoint. In those cases an
            :class:`ExplorerFetchError` is raised with details.

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

        try:
            tb = _load_chart_table(
                self.slug,
                load_data=load_data,
                timeout=self._timeout,
                use_column_short_names=True,
                base_url=self.chart_base_url,
                extra_params=_parse_query_params(self.query_params) if self.query_params else None,
            )
        except (requests.HTTPError, ChartNotFoundError) as e:
            if self.type == EXPLORER:
                raise ExplorerFetchError(
                    f"Failed to fetch explorer view '{self.slug}'. "
                    f"Explorer CSV endpoints are not always available. "
                    f"Try fetching the underlying chart directly. Error: {e}"
                ) from e
            raise

        # Cache only if loading full data with default params
        if load_data:
            self._cached_chart_table = tb

        return tb

    @property
    def description(self) -> str:
        """Return a string description of the chart result."""
        return self.subtitle


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

    def __init__(self, client: "Client", site_url: str) -> None:
        """Initialize the ChartsAPI.

        Args:
            client: The Client instance.
            site_url: Base URL for the OWID website (e.g., "https://ourworldindata.org").
        """
        self._client = client
        self._site_url = site_url

    @property
    def base_url(self) -> str:
        """Base URL for the Grapher (read-only)."""
        return f"{self._site_url}/grapher"

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
        )

    def fetch(
        self,
        slug_or_url: str,
        *,
        type: ChartType | None = None,
        load_data: bool = True,
        timeout: int | None = None,
    ) -> ChartTable:
        """Fetch chart data as a ChartTable with rich metadata.

        Accepts a chart slug, a slug with query parameters, or a full URL. The slug,
        query parameters, and chart type are extracted automatically.

        Args:
            slug_or_url: One of:

                - Chart slug: ``"life-expectancy"``
                - Slug with query params: ``"education-spending?level=primary&spending_type=gdp_share"``
                - Full grapher URL: ``"https://ourworldindata.org/grapher/life-expectancy?tab=table"``
                - Full explorer URL: ``"https://ourworldindata.org/explorers/covid?Metric=Cases"``
            type: Override the chart type. Defaults to ``"chart"`` (grapher).
                Use ``"explorerView"`` for explorer views. Auto-detected from full URLs.
            load_data: If True (default), load full chart data.
                       If False, load only structure (columns and metadata) without rows.
            timeout: HTTP request timeout in seconds. Defaults to client timeout.

        Returns:
            ChartTable with chart data and chart_config. Column metadata (unit, description, etc.)
            is populated from the chart's metadata.json. Chart config is accessible via .metadata.chart_config.

        Note:
            Explorer views are best-effort. Some explorers may return 503 or other errors
            from their CSV endpoint.

        Example:
            ```python
            # Fetch a grapher chart by slug
            tb = client.charts.fetch("life-expectancy")

            # Fetch with query params (e.g., a multiDim view)
            tb = client.charts.fetch("education-spending?level=primary&spending_type=gdp_share")

            # Fetch from a full URL (type and query params auto-detected)
            tb = client.charts.fetch("https://ourworldindata.org/explorers/covid?Metric=Cases")

            # Explicitly fetch an explorer view
            tb = client.charts.fetch("covid?Metric=Cases", type="explorerView")
            ```
        """
        effective_timeout = timeout or self._client.timeout
        parsed = parse_chart_slug(slug_or_url)

        slug = parsed.slug
        effective_type = type or parsed.type
        effective_params = parsed.query_params

        # Pick base URL based on type
        if effective_type == EXPLORER:
            base_url = f"{self._site_url}/explorers"
            other_type: ChartType = CHART
        else:
            base_url = self.base_url
            other_type = EXPLORER

        try:
            return _load_chart_table(
                slug,
                load_data=load_data,
                timeout=effective_timeout,
                use_column_short_names=True,
                base_url=base_url,
                extra_params=_parse_query_params(effective_params) if effective_params else None,
            )
        except (requests.HTTPError, ChartNotFoundError) as e:
            hint = "an explorer view" if other_type == EXPLORER else "a grapher chart"
            raise e.__class__(f"{e}\n\nIf this is {hint}, " f'try: fetch("{slug_or_url}", type={other_type!r})') from e

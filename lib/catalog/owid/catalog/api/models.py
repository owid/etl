#
#  owid.catalog.client.models
#
#  Pydantic model definitions for API responses.
#
from __future__ import annotations

import io
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any, Callable, Generic, Iterator, TypeVar

import pandas as pd
import requests
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from owid.catalog.api.utils import _loading_data_from_api
from owid.catalog.core import CatalogPath
from owid.catalog.tables import Table

if TYPE_CHECKING:
    from owid.catalog.api.catalogs import CatalogFrame
    from owid.catalog.variables import Variable

T = TypeVar("T")


class ChartNotFoundError(Exception):
    """Raised when a chart does not exist."""

    pass


class LicenseError(Exception):
    """Raised when chart data cannot be downloaded due to licensing."""

    pass


def _load_table(
    path: str,
    formats: list[str] | None = None,
    is_public: bool = True,
    load_data: bool = True,
) -> "Table":
    """Load a table from the catalog by path.

    Helper function for loading table data. Used by TableResult and IndicatorResult.

    Args:
        path: Table path in catalog (e.g., "grapher/namespace/version/dataset/table")
        formats: List of formats to try. If None, tries all supported formats.
        is_public: Whether the table is publicly accessible.
        load_data: If True, load full data. If False, load only table structure (columns and metadata) without rows.

    Returns:
        Table object with data and metadata (or just metadata if load_data=False).

    Raises:
        KeyError: If no table found at the specified path.
    """
    import tempfile

    from owid.catalog.api.catalogs import download_private_file_s3
    from owid.catalog.api.utils import (
        OWID_CATALOG_URI,
        PREFERRED_FORMAT,
        SUPPORTED_FORMATS,
    )

    # Extract table name for display
    catalog_path = CatalogPath.from_str(path)
    table_name = catalog_path.table or path
    message = f"Loading table '{table_name}'"

    def fct():
        base_uri = OWID_CATALOG_URI
        uri = "/".join([base_uri.rstrip("/"), path])

        # Determine format preference
        if formats:
            formats_to_try = formats
        else:
            formats_to_try = SUPPORTED_FORMATS

        # Prefer feather if available
        if PREFERRED_FORMAT in formats_to_try:
            formats_to_try = [PREFERRED_FORMAT] + [f for f in formats_to_try if f != PREFERRED_FORMAT]

        for fmt in formats_to_try:
            try:
                table_uri = f"{uri}.{fmt}"

                # Handle private files
                if not is_public:
                    tmpdir = tempfile.mkdtemp()
                    table_uri = download_private_file_s3(table_uri, tmpdir)

                # If header_only, return empty table with same structure
                return Table.read(table_uri, load_data=load_data)
            except Exception:
                continue

        raise KeyError(f"No matching table found at: {path}")

    if load_data:
        with _loading_data_from_api(message):
            return fct()
    else:
        return fct()


class ChartResult(BaseModel):
    """An OWID chart (from fetch or search).

    Fields populated depend on the source:
    - fetch(): Provides config and metadata
    - search(): Provides subtitle, available_entities, and num_related_articles

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

    # Private cached data field
    _data: pd.DataFrame | None = PrivateAttr(default=None)

    @property
    def data(self) -> pd.DataFrame:
        """Lazy-load chart data. Data is cached after first access.

        Returns:
            DataFrame with chart data. Metadata is available in df.attrs.
        """
        if self._data is None:
            self._data = self._load_data()
        return self._data

    def _load_data(self) -> pd.DataFrame:
        """Internal method to fetch chart data."""
        with _loading_data_from_api(f"Fetching chart '{self.slug}'"):
            # Fetch CSV data from the chart
            url = f"https://ourworldindata.org/grapher/{self.slug}.csv?useColumnShortNames=true"
            resp = requests.get(url)

            if resp.status_code == 404:
                raise ChartNotFoundError(f"No such chart found: {self.slug}")

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
            df.attrs["slug"] = self.slug
            df.attrs["url"] = self.url

            # Rename "years" to "dates" if values are date strings
            if "years" in df.columns and df["years"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$").all():
                df = df.rename(columns={"years": "dates"})

            return df


class PageSearchResult(BaseModel):
    """An article/page found via search.

    Attributes:
        slug: Page URL identifier.
        title: Page title.
        url: Full URL to the page.
        excerpt: Short excerpt from the page content.
        authors: List of author names.
        published_at: Publication date string.
        thumbnail_url: URL to thumbnail image.
    """

    slug: str
    title: str
    url: str
    excerpt: str = ""
    authors: list[str] = Field(default_factory=list)
    published_at: str = ""
    thumbnail_url: str = ""


class IndicatorResult(BaseModel):
    """An indicator found via semantic search.

    Attributes:
        indicator_id: Unique indicator ID.
        title: Indicator title/name.
        score: Semantic similarity score (0-1).
        catalog_path: Path in the catalog (e.g., "grapher/un/2024/pop/pop#population").
        description: Full indicator description.
        column_name: Column name in the table.
        unit: Unit of measurement.
        n_charts: Number of charts using this indicator.
        dataset: Dataset name (parsed from catalog_path).
        version: Version string (parsed from catalog_path).
        namespace: Data provider namespace (parsed from catalog_path).
        channel: Data channel (parsed from catalog_path).
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    indicator_id: int | None
    title: str
    score: float
    catalog_path: str | None
    description: str = ""
    column_name: str = ""
    unit: str = ""
    n_charts: int | None = None
    dataset: str | None = None
    version: str | None = None
    namespace: str | None = None
    channel: str | None = None
    _table: "Table | None" = PrivateAttr(default=None)
    _legacy: bool = PrivateAttr(default=False)

    def model_post_init(self, __context: Any) -> None:
        """Parse dataset, version, namespace, channel from catalog_path."""
        if self.catalog_path and not self.dataset:
            # Parse using CatalogPath
            try:
                # CatalogPath.from_str() handles the "#" automatically
                parsed = CatalogPath.from_str(self.catalog_path)
                # Set parsed fields
                object.__setattr__(self, "dataset", parsed.dataset)
                object.__setattr__(self, "version", parsed.version)
                object.__setattr__(self, "namespace", parsed.namespace)
                object.__setattr__(self, "channel", parsed.channel)
            except Exception:
                # If parsing fails, leave fields empty
                object.__setattr__(self, "_legacy", True)

    @property
    def data(self) -> "Variable":
        """Lazy-load indicator data as a Variable (Series). Data is cached after first access.

        Returns:
            Variable object (pandas Series subclass) with the indicator data.
        """

        if self._table is None:
            self._table = self._load_table()
        # Extract the specific column/variable
        return self._table[self.column_name].dropna()  # type: ignore

    @property
    def table(self) -> "Table":
        """Lazy-load the full table containing this indicator.

        Returns:
            Table object with all columns including this indicator.
        """
        if self._table is None:
            self._table = self._load_table()
        return self._table

    def _load_table(self) -> "Table":
        """Internal method to load the table containing this indicator."""
        # Parse catalog_path using CatalogPath
        parsed = CatalogPath.from_str(self.catalog_path)
        # Use table_path property (without variable)
        if parsed.table_path is None:
            raise ValueError(f"Invalid catalog path: {self.catalog_path}")
        return _load_table(parsed.table_path)


class TableResult(BaseModel):
    """A table found in the catalog.

    Attributes:
        table: Table name.
        dataset: Dataset name.
        version: Version string.
        namespace: Data provider namespace.
        channel: Data channel (garden, meadow, etc.).
        path: Full path to the table.
        is_public: Whether the data is publicly accessible.
        dimensions: List of dimension columns.
        formats: List of available formats.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    table: str
    dataset: str
    version: str
    namespace: str
    channel: str
    path: str
    is_public: bool = True
    dimensions: list[str] = Field(default_factory=list)
    formats: list[str] = Field(default_factory=list)
    _data: "Table | None" = PrivateAttr(default=None)

    @property
    def data(self) -> "Table":
        """Lazy-load table data. Data is cached after first access.

        Returns:
            Table object with data and metadata.
        """
        if self._data is None:
            self._data = self._load()
        return self._data

    @property
    def data_header(self) -> "Table":
        """Load only the table structure (columns and metadata) without rows.

        This is useful for quickly accessing metadata without loading potentially large datasets.

        Returns:
            Empty Table object with columns and metadata but no data rows.
        """
        return _load_table(self.path, formats=self.formats, is_public=self.is_public, load_data=False)

    def _load(self) -> "Table":
        """Internal method to load table data."""
        return _load_table(self.path, formats=self.formats, is_public=self.is_public)

    def experimental_preview(self, n: int = 10) -> "Table":
        """Preview first N rows without loading full dataset (experimental feature).

        This is useful for quickly inspecting large tables without loading all data into memory.
        Note: This is an experimental feature and may change in future versions.

        Args:
            n: Number of rows to preview (default: 10).

        Returns:
            Table with first N rows and full metadata.

        Example:
            >>> result = client.tables.fetch("garden/un/2024-07-12/un_wpp/population")
            >>> preview = result.experimental_preview(n=5)
            >>> print(preview.shape)
            (5, 3)  # Only 5 rows loaded
        """
        # Load the full table (unfortunately, we need to load all data to get first N rows
        # as the underlying storage format doesn't support partial reads efficiently)
        # TODO: Optimize this by adding support for row-limited reads in Table.read()
        table = self.data
        preview = table.head(n)
        # Ensure we return a Table, not DataFrame/Series
        from owid.catalog.tables import Table as TableClass

        if not isinstance(preview, TableClass):
            preview = TableClass(preview)
        return preview

    def experimental_summary(self) -> str:
        """Get summary statistics without loading full dataset (experimental feature).

        Returns metadata and structure information (shape, dtypes, memory usage) without
        loading actual data rows. This is much faster than loading the full table.

        Note: This is an experimental feature and may change in future versions.

        Returns:
            Formatted string with table summary (shape, dtypes, null counts, memory estimate).

        Example:
            >>> result = client.tables.fetch("garden/un/2024-07-12/un_wpp/population")
            >>> print(result.experimental_summary())
            Table: population
            Dataset: un_wpp (garden/un/2024-07-12)
            Dimensions: ['country', 'year']
            Columns: 3
            Dtypes: int64(2), float64(1)
            Memory: ~45.8 MB (estimated)
        """
        # Use data_header to get structure without loading rows
        header = self.data_header

        # Build summary string
        lines = [
            f"Table: {self.table}",
            f"Dataset: {self.dataset} ({self.channel}/{self.namespace}/{self.version})",
        ]

        if self.dimensions:
            lines.append(f"Dimensions: {self.dimensions}")

        lines.append(f"Columns: {len(header.columns)}")

        # Get dtype counts
        dtype_counts: dict[str, int] = {}
        for dtype in header.dtypes:
            dtype_str = str(dtype)
            dtype_counts[dtype_str] = dtype_counts.get(dtype_str, 0) + 1

        dtype_summary = ", ".join([f"{dtype}({count})" for dtype, count in sorted(dtype_counts.items())])
        lines.append(f"Dtypes: {dtype_summary}")

        # Note: Can't get actual memory without loading data, but can estimate
        lines.append("Memory: Unknown (call .data to load and measure)")

        return "\n".join(lines)

    def experimental_describe_metadata(self) -> str:
        """Pretty-print table metadata (experimental feature).

        Returns a formatted string with the table's metadata including title, description,
        sources, and other relevant information.

        Note: This is an experimental feature and may change in future versions.

        Returns:
            Formatted string with metadata details.

        Example:
            >>> result = client.tables.fetch("garden/un/2024-07-12/un_wpp/population")
            >>> print(result.experimental_describe_metadata())
            === Table Metadata ===
            Title: Population by country and year
            Description: Total population by country...
            Sources: UN World Population Prospects (2024)
            ...
        """
        # Load header to get metadata
        header = self.data_header
        metadata = header.metadata

        lines = ["=== Table Metadata ==="]

        if metadata.title:
            lines.append(f"Title: {metadata.title}")

        if metadata.description:
            # Truncate long descriptions
            desc = metadata.description
            if len(desc) > 200:
                desc = desc[:200] + "..."
            lines.append(f"Description: {desc}")

        # Sources and licenses are on the dataset, not the table
        if metadata.dataset:
            if metadata.dataset.sources:
                source_names = [s.name for s in metadata.dataset.sources if s.name]
                if source_names:
                    lines.append(f"Sources: {', '.join(source_names)}")

            if metadata.dataset.licenses:
                license_names = [lic.name for lic in metadata.dataset.licenses if lic.name]
                if license_names:
                    lines.append(f"Licenses: {', '.join(license_names)}")

        # Add column information
        lines.append(f"\nColumns ({len(header.columns)}):")
        for col_name in header.columns:
            col = header[col_name]
            col_info = f"  - {col_name}"
            if hasattr(col, "metadata") and col.metadata:
                if col.metadata.unit:
                    col_info += f" ({col.metadata.unit})"
                if col.metadata.title and col.metadata.title != col_name:
                    col_info += f": {col.metadata.title}"
            lines.append(col_info)

        return "\n".join(lines)


class ResponseSet(BaseModel, Generic[T]):
    """Generic container for API responses.

    Provides iteration, indexing, and conversion to CatalogFrame
    for backwards compatibility.

    Attributes:
        results: List of result objects.
        query: The query that produced these results.
        limit: Total number of results (may be more than len(results)).
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    results: list[T]
    query: str = ""
    limit: int = 0

    def _get_type_display(self) -> str:
        """Get display name for ResponseSet with generic type."""
        if not self.results:
            return "ResponseSet"

        # Get the type of the first result
        first_result = self.results[0]
        type_name = type(first_result).__name__
        return f"ResponseSet[{type_name}]"

    def model_post_init(self, __context: Any) -> None:
        """Set limit to length of results if not provided."""
        if self.limit == 0:
            self.limit = len(self.results)

    def __iter__(self) -> Iterator[T]:  # type: ignore[override]
        """Iterate over results, not model fields."""
        return iter(self.results)

    def __len__(self) -> int:
        return len(self.results)

    def __getitem__(self, index: int) -> T:
        return self.results[index]

    def __repr__(self) -> str:
        """Display results as a formatted table for better readability."""
        type_display = self._get_type_display()

        if not self.results:
            return f"{type_display}(query={self.query!r}, limit=0, results=[])"

        # Convert to DataFrame for nice tabular display
        df = self.to_frame()

        # Limit display to first 10 rows for readability
        if len(df) == 0:
            return f"{type_display}(query={self.query!r}, limit={self.limit}, results=[])"
        else:
            df_str = str(df)

        # Format as bullet points to show attributes at same level
        # Indent DataFrame lines to align with bullet points
        df_lines = df_str.split("\n")
        indented_df = "\n    ".join(df_lines)

        header = f"{type_display}\n.query={self.query!r}\n.limit={self.limit}\n.results:\n    {indented_df}"

        # Add helper tip at the end
        tip = "\n\nTip: Use .to_frame() for pandas operations, or .latest(by='field') to get most recent"
        return header + tip

    def __str__(self) -> str:
        """Use the same representation for str() and repr()."""
        return self.__repr__()

    def _repr_html_(self) -> str:
        """Display as HTML table in Jupyter notebooks."""
        type_display = self._get_type_display()

        if not self.results:
            return f"<p>{type_display}(query={self.query!r}, limit=0, results=[])</p>"

        df = self.to_frame()
        df_html = df._repr_html_()

        # Format as bullet points to show attributes at same level
        html = f"""<div>
  <p><strong>{type_display}</strong></p>
  <ul style="list-style-type: none; padding-left: 1em;">
    <li><strong>.query</strong>: {self.query!r}</li>
    <li><strong>.limit</strong>: {self.limit}</li>
    <li><strong>.results</strong>:
      <div style="margin-left: 1.5em; margin-top: 0.5em;">
        {df_html}
      </div>
    </li>
  </ul>
</div>"""
        return html

    def to_frame(self) -> pd.DataFrame:
        """Convert results to a DataFrame.

        Returns:
            DataFrame with one row per result.
        """
        if not self.results:
            return pd.DataFrame()

        # Convert Pydantic models to dicts
        rows = []
        for r in self.results:
            if isinstance(r, BaseModel):
                # For ChartResult, exclude large dict fields for better display
                if isinstance(r, ChartResult):
                    row = {
                        "slug": r.slug,
                        "title": r.title,
                        "subtitle": r.subtitle,
                        "url": r.url,
                        "num_related_articles": r.num_related_articles,
                        # Only show count of entities, not full list
                        "num_entities": len(r.available_entities),
                    }
                else:
                    row = r.model_dump()
                rows.append(row)
            else:
                rows.append(r)

        return pd.DataFrame(rows)

    def to_catalog_frame(self) -> "CatalogFrame":
        """Convert to CatalogFrame for backwards compatibility.

        Only works for TableResult and IndicatorResult types.

        Returns:
            CatalogFrame that can use .load() method.
        """
        from owid.catalog.api.catalogs import CatalogFrame as CF
        from owid.catalog.api.utils import OWID_CATALOG_URI

        if not self.results:
            return CF.create_empty()

        # Check result type
        first = self.results[0]
        if isinstance(first, TableResult):
            rows = []
            for r in self.results:
                rows.append(
                    {
                        "table": r.table,  # type: ignore
                        "dataset": r.dataset,  # type: ignore
                        "version": r.version,  # type: ignore
                        "namespace": r.namespace,  # type: ignore
                        "channel": r.channel,  # type: ignore
                        "path": r.path,  # type: ignore
                        "is_public": r.is_public,  # type: ignore
                        "dimensions": r.dimensions,  # type: ignore
                        "format": r.formats[0] if r.formats else "feather",  # type: ignore
                    }
                )
            frame = CF(rows)
            frame._base_uri = OWID_CATALOG_URI
            return frame

        elif isinstance(first, IndicatorResult):
            rows = []
            for r in self.results:
                # Parse catalog path using CatalogPath
                try:
                    parsed = CatalogPath.from_str(r.catalog_path)  # type: ignore
                    indicator = parsed.variable or ""
                    channel = parsed.channel
                    namespace = parsed.namespace
                    version = parsed.version
                    dataset = parsed.dataset
                    table = parsed.table or dataset
                    # Use table_path property (without variable)
                    path_part = parsed.table_path or parsed.dataset_path
                except Exception:
                    # Fallback if parsing fails
                    indicator = channel = namespace = version = dataset = table = ""
                    path_part = r.catalog_path.split("#")[0] if "#" in r.catalog_path else r.catalog_path  # type: ignore

                rows.append(
                    {
                        "indicator_title": r.title,  # type: ignore
                        "indicator": indicator,
                        "score": r.score,  # type: ignore
                        "table": table,
                        "dataset": dataset,
                        "version": version,
                        "namespace": namespace,
                        "channel": channel,
                        "is_public": True,
                        "path": path_part,
                        "format": "parquet",
                    }
                )
            frame = CF(rows)
            frame._base_uri = OWID_CATALOG_URI
            return frame

        else:
            raise TypeError(f"Cannot convert {type(first).__name__} results to CatalogFrame")

    def filter(self, predicate: Callable[[T], bool]) -> "ResponseSet[T]":
        """Filter results by predicate function.

        Returns a new ResponseSet with only items that match the predicate.
        The predicate should return True for items to keep.

        Args:
            predicate: Function that takes an item and returns True/False.

        Returns:
            New ResponseSet with filtered results.

        Example:
            >>> # Filter results by version
            >>> results.filter(lambda r: r.version > '2024')
            >>>
            >>> # Filter by namespace
            >>> results.filter(lambda r: r.namespace == "worldbank")
            >>>
            >>> # Chain multiple filters
            >>> results.filter(lambda r: r.version > '2024').filter(lambda r: r.namespace == "un")
        """
        filtered_results = [item for item in self.results if predicate(item)]
        return ResponseSet(
            results=filtered_results,
            query=self.query,
            limit=len(filtered_results),
        )

    def sort_by(self, key: str | Callable[[T], Any], *, reverse: bool = False) -> "ResponseSet[T]":
        """Sort results by attribute name or key function.

        Returns a new ResponseSet with items sorted by the specified key.

        Args:
            key: Either an attribute name (string) or a function that extracts a comparison key from each item.
            reverse: If True, sort in descending order (default: False).

        Returns:
            New ResponseSet with sorted results.

        Example:
            >>> # Sort by version (ascending)
            >>> results.sort_by('version')
            >>>
            >>> # Sort by version (descending - latest first)
            >>> results.sort_by('version', reverse=True)
            >>>
            >>> # Sort by custom function (e.g., by score)
            >>> results.sort_by(lambda r: r.score, reverse=True)
            >>>
            >>> # Chain sorting and filtering
            >>> results.filter(lambda r: r.version > '2024').sort_by('version', reverse=True)
        """
        if isinstance(key, str):
            # Sort by attribute name
            sorted_results = sorted(self.results, key=lambda item: getattr(item, key), reverse=reverse)
        else:
            # Sort by key function
            sorted_results = sorted(self.results, key=key, reverse=reverse)

        return ResponseSet(
            results=sorted_results,
            query=self.query,
            limit=self.limit,
        )

    def latest(self, by: str = "version") -> T:
        """Get the most recent result by a specific field.

        Returns the single item with the highest value for the specified field.

        Args:
            by: Attribute name to sort by (default: 'version').
                Common values: 'version', 'published_at', 'score'.

        Returns:
            Single item with the highest value for the specified field.

        Raises:
            ValueError: If no results are available.
            AttributeError: If the specified attribute doesn't exist on the results.

        Example:
            >>> # For TableResult - use version (default)
            >>> latest_table = results.latest()
            >>> tb = latest_table.data
            >>>
            >>> # For IndicatorResult - use version (parsed from catalog_path)
            >>> latest_indicator = results.latest()
            >>>
            >>> # For PageSearchResult - use published_at
            >>> latest_article = results.latest(by='published_at')
            >>>
            >>> # For IndicatorResult - sort by relevance score
            >>> best_match = results.latest(by='score')
        """
        if not self.results:
            raise ValueError("No results available to get latest from")

        # Check if attribute exists on first item
        if not hasattr(self.results[0], by):
            # Get available attributes (exclude private ones)
            available = [
                k for k in dir(self.results[0]) if not k.startswith("_") and not callable(getattr(self.results[0], k))
            ]
            raise AttributeError(
                f"Results don't have '{by}' attribute. " f"Available attributes: {', '.join(sorted(available))}"
            )

        return max(self.results, key=lambda item: getattr(item, by))

    def first(self, n: int = 1) -> T | "ResponseSet[T]":
        """Get the first n results.

        Args:
            n: Number of results to return (default: 1).

        Returns:
            If n=1, returns a single item (or None if no results).
            If n>1, returns a new ResponseSet with the first n results.

        Example:
            >>> # Get first result
            >>> first_result = results.first()
            >>> tb = first_result.data
            >>>
            >>> # Get first 5 results
            >>> top_five = results.first(5)
            >>>
            >>> # Combine with sorting
            >>> latest_five = results.sort_by('version', reverse=True).first(5)
        """
        if n == 1:
            return self.results[0] if self.results else None  # type: ignore
        else:
            return ResponseSet(
                results=self.results[:n],
                query=self.query,
                limit=self.limit,
            )

    def experimental_download_all(
        self, *, parallel: bool = True, max_workers: int = 4, show_progress: bool = True
    ) -> dict[str, Table | Exception]:
        """Download data for all results in parallel (experimental feature).

        This is useful for bulk downloading multiple tables efficiently.
        Note: This is an experimental feature and may change in future versions.

        Args:
            parallel: If True, download in parallel using ThreadPoolExecutor (default: True).
            max_workers: Maximum number of concurrent downloads (default: 4).
            show_progress: If True, show progress bar (default: True).

        Returns:
            Dictionary mapping table names to loaded Table objects or Exception if download failed.

        Example:
            >>> results = client.tables.search("worldbank/wdi")
            >>> tbs = results.experimental_download_all(parallel=True, max_workers=4)
            >>> # Check which downloads succeeded
            >>> successful = {k: v for k, v in tables.items() if not isinstance(v, Exception)}
            >>> failed = {k: v for k, v in tables.items() if isinstance(v, Exception)}
        """
        # Only works for TableResult types
        if not self.results:
            return {}

        # Check if this is a TableResult ResponseSet
        if not isinstance(self.results[0], TableResult):
            raise ValueError("experimental_download_all() only works with TableResult objects")

        output: dict[str, Table | Exception] = {}

        def download_one(result: TableResult) -> tuple[str, Table | Exception]:
            """Download a single table, returning name and data or exception."""
            try:
                data = result.data
                return (result.table, data)
            except Exception as e:
                return (result.table, e)

        if parallel:
            # Parallel download using ThreadPoolExecutor
            try:
                from tqdm import tqdm

                has_tqdm = True
            except ImportError:
                has_tqdm = False
                if show_progress:
                    print("Install tqdm for progress bars: uv add tqdm")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all download tasks
                futures = {executor.submit(download_one, result): result for result in self.results}

                # Collect results with optional progress bar
                if show_progress and has_tqdm:
                    with tqdm(total=len(futures), desc="Downloading tables") as pbar:
                        for future in as_completed(futures):
                            name, data = future.result()
                            output[name] = data
                            pbar.update(1)
                else:
                    for future in as_completed(futures):
                        name, data = future.result()
                        output[name] = data
        else:
            # Sequential download
            try:
                from tqdm import tqdm

                has_tqdm = True
            except ImportError:
                has_tqdm = False

            iterator = tqdm(self.results, desc="Downloading tables") if show_progress and has_tqdm else self.results

            for result in iterator:
                name, data = download_one(result)
                output[name] = data

        return output

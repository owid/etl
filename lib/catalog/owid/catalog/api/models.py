#
#  owid.catalog.client.models
#
#  Pydantic model definitions for API responses.
#
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Iterator, TypeVar

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

if TYPE_CHECKING:
    from owid.catalog.catalogs import CatalogFrame
    from owid.catalog.tables import Table
    from owid.catalog.variables import Variable

T = TypeVar("T")


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
            self._data = self.get_data()
        return self._data

    def get_data(self) -> pd.DataFrame:
        """Fetch the data for this chart.

        Returns:
            DataFrame with chart data. Metadata is available in df.attrs.
        """
        # Import here to avoid circular imports
        from .charts import ChartsAPI

        return ChartsAPI._fetch_data(self.slug)


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
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    indicator_id: int
    title: str
    score: float
    catalog_path: str
    description: str = ""
    column_name: str = ""
    unit: str = ""
    n_charts: int = 0
    _table: "Table | None" = PrivateAttr(default=None)

    @property
    def data(self) -> "Variable":
        """Lazy-load indicator data as a Variable (Series). Data is cached after first access.

        Returns:
            Variable object (pandas Series subclass) with the indicator data.
        """

        if self._table is None:
            self._table = self._load()
        # Extract the specific column/variable
        return self._table[self.column_name]  # type: ignore

    @property
    def table(self) -> "Table":
        """Lazy-load the full table containing this indicator.

        Returns:
            Table object with all columns including this indicator.
        """
        if self._table is None:
            self._table = self._load()
        return self._table

    def _load(self) -> "Table":
        """Internal method to load the table containing this indicator."""
        from .tables import TablesAPI

        # Parse catalog_path: "grapher/namespace/version/dataset/table#column"
        path_part, _, _ = self.catalog_path.partition("#")
        return TablesAPI._load_table(path_part)


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

    def _load(self) -> "Table":
        """Internal method to load table data."""
        from .tables import TablesAPI

        return TablesAPI._load_table(self.path, formats=self.formats, is_public=self.is_public)


class ResultSet(BaseModel, Generic[T]):
    """Generic container for API results.

    Provides iteration, indexing, and conversion to CatalogFrame
    for backwards compatibility.

    Attributes:
        results: List of result objects.
        query: The query that produced these results.
        total: Total number of results (may be more than len(results)).
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    results: list[T]
    query: str = ""
    total: int = 0

    def model_post_init(self, __context: Any) -> None:
        """Set total to length of results if not provided."""
        if self.total == 0:
            self.total = len(self.results)

    def __iter__(self) -> Iterator[T]:  # type: ignore[override]
        """Iterate over results, not model fields."""
        return iter(self.results)

    def __len__(self) -> int:
        return len(self.results)

    def __getitem__(self, index: int) -> T:
        return self.results[index]

    def __repr__(self) -> str:
        """Display results as a formatted table for better readability."""
        if not self.results:
            return f"ResultSet(query={self.query!r}, total=0, results=[])"

        # Convert to DataFrame for nice tabular display
        df = self.to_frame()

        # Limit display to first 10 rows for readability
        if len(df) == 0:
            return f"ResultSet(query={self.query!r}, total={self.total}, results=[])"
        elif len(df) <= 10:
            df_str = str(df)
        else:
            df_str = str(df.head(10))

        # Format as bullet points to show attributes at same level
        # Indent DataFrame lines to align with bullet points
        df_lines = df_str.split("\n")
        indented_df = "\n    ".join(df_lines)

        header = f"ResultSet\n  • query={self.query!r}\n  • total={self.total}\n  • .results:\n    {indented_df}"
        return header

    def __str__(self) -> str:
        """Use the same representation for str() and repr()."""
        return self.__repr__()

    def _repr_html_(self) -> str:
        """Display as HTML table in Jupyter notebooks."""
        if not self.results:
            return f"<p>ResultSet(query={self.query!r}, total=0, results=[])</p>"

        df = self.to_frame()
        if len(df) <= 10:
            df_html = df._repr_html_()
        else:
            df_html = df.head(10)._repr_html_()

        # Format as bullet points to show attributes at same level
        html = f"""<div>
  <p><strong>ResultSet</strong></p>
  <ul style="list-style-type: none; padding-left: 1em;">
    <li>• <strong>query</strong>: {self.query!r}</li>
    <li>• <strong>total</strong>: {self.total}</li>
    <li>• <strong>.results</strong>:
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
        from .utils import OWID_CATALOG_URI
        from .utils import CatalogFrame as CF

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
                path_part, _, indicator = r.catalog_path.partition("#")  # type: ignore
                parts = path_part.split("/")

                if len(parts) >= 4:
                    channel, namespace, version, dataset = parts[0], parts[1], parts[2], parts[3]
                    table = parts[4] if len(parts) > 4 else dataset
                else:
                    channel = namespace = version = dataset = table = ""

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

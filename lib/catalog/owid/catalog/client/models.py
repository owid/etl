#
#  owid.catalog.client.models
#
#  Pydantic model definitions for API responses.
#
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Iterator, TypeVar

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from ..catalogs import CatalogFrame
    from ..tables import Table

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

    def load(self) -> "Table":
        """Load the table containing this indicator.

        Returns:
            Table object with the indicator data.
        """
        from .datasets import DatasetsAPI

        # Parse catalog_path: "grapher/namespace/version/dataset/table#column"
        path_part, _, _ = self.catalog_path.partition("#")
        return DatasetsAPI._load_table(path_part)


class DatasetResult(BaseModel):
    """A dataset found in the catalog.

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

    def load(self) -> "Table":
        """Load this table from the catalog.

        Returns:
            Table object with data and metadata.
        """
        from .datasets import DatasetsAPI

        return DatasetsAPI._load_table(self.path, formats=self.formats, is_public=self.is_public)


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
        return f"ResultSet(query={self.query!r}, total={self.total}, results={self.results})"

    def to_frame(self) -> pd.DataFrame:
        """Convert results to a DataFrame.

        Returns:
            DataFrame with one row per result.
        """
        if not self.results:
            return pd.DataFrame()

        # Convert Pydantic models to dicts
        rows = [r.model_dump() if isinstance(r, BaseModel) else r for r in self.results]  # type: ignore
        return pd.DataFrame(rows)

    def to_catalog_frame(self) -> "CatalogFrame":
        """Convert to CatalogFrame for backwards compatibility.

        Only works for DatasetResult and IndicatorResult types.

        Returns:
            CatalogFrame that can use .load() method.
        """
        from ..catalogs import OWID_CATALOG_URI
        from ..catalogs import CatalogFrame as CF

        if not self.results:
            return CF.create_empty()

        # Check result type
        first = self.results[0]
        if isinstance(first, DatasetResult):
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

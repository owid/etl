#
#  owid.catalog.client.models
#
#  Pydantic model definitions for API responses.
#
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Generic, Iterator, TypeVar

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from owid.catalog.api.catalogs import CatalogFrame

T = TypeVar("T")


class ResponseSet(BaseModel, Generic[T]):
    """Generic container for API responses.

    Provides iteration, indexing, and conversion to CatalogFrame
    for backwards compatibility.

    Attributes:
        results: List of result objects.
        query: The query that produced these results.
        total_count: Total number of results available (may be more than len(results)).
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    results: list[T]
    query: str = ""
    total_count: int = 0
    base_url: str = Field(frozen=True)

    def _get_type_display(self) -> str:
        """Get display name for ResponseSet with generic type."""
        if not self.results:
            return "ResponseSet"

        # Get the type of the first result
        first_result = self.results[0]
        type_name = type(first_result).__name__
        return f"ResponseSet[{type_name}]"

    def model_post_init(self, __context: Any) -> None:
        """Set total_count to length of results if not provided."""
        if self.total_count == 0:
            self.total_count = len(self.results)

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
            return f"{type_display}(query={self.query!r}, total_count=0, results=[])"

        # Convert to DataFrame for nice tabular display
        df = self.to_frame()

        # Limit display to first 10 rows for readability
        if len(df) == 0:
            return f"{type_display}(query={self.query!r}, total_count={self.total_count}, results=[])"
        else:
            df_str = str(df)

        # Format as bullet points to show attributes at same level
        # Indent DataFrame lines to align with bullet points
        df_lines = df_str.split("\n")
        indented_df = "\n    ".join(df_lines)

        header = f"{type_display}\n.query={self.query!r}\n.total_count={self.total_count}\n.results:\n    {indented_df}"

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
    <li><strong>.total_count</strong>: {self.total_count}</li>
    <li><strong>.results</strong>:
      <div style="margin-left: 1.5em; margin-top: 0.5em;">
        {df_html}
      </div>
    </li>
  </ul>
</div>"""
        return html

    def latest(self, by: str | None = None) -> T:
        """Get the most recent result.

        Returns the single item with the highest value for the sort key.

        Args:
            by: Attribute name to sort by. If None (default), auto-detects:
                - ChartResult: uses last_updated (as ISO string with time)
                - TableResult/IndicatorResult: uses version

        Returns:
            Single item with the highest value for the specified field.

        Raises:
            ValueError: If no results are available.
            AttributeError: If the specified attribute doesn't exist on the results.

        Example:
            ```py
            >>> # For TableResult/IndicatorResult - auto-detects version
            >>> latest_table = results.latest()
            >>> tb = latest_table.fetch()

            >>> # For ChartResult - auto-detects last_updated
            >>> latest_chart = chart_results.latest()
            ```
        """
        if not self.results:
            raise ValueError("No results available to get latest from")

        # Auto-detect sort key based on result type
        if by is None:
            return max(self.results, key=self._get_version_string)

        # Explicit attribute name
        if not hasattr(self.results[0], by):
            # Get available attributes (exclude private ones)
            available = [
                k for k in dir(self.results[0]) if not k.startswith("_") and not callable(getattr(self.results[0], k))
            ]
            raise AttributeError(
                f"Results don't have '{by}' attribute. " f"Available attributes: {', '.join(sorted(available))}"
            )

        return max(self.results, key=lambda item: getattr(item, by))

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
                # Use type name check to avoid circular imports
                if type(r).__name__ == "ChartResult":
                    row = {
                        "slug": getattr(r, "slug", ""),
                        "title": getattr(r, "title", ""),
                        "subtitle": getattr(r, "subtitle", ""),
                        "url": getattr(r, "url", ""),
                        "num_related_articles": getattr(r, "num_related_articles", 0),
                        # Only show count of entities, not full list
                        "num_entities": len(getattr(r, "available_entities", [])),
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
        from owid.catalog.core import CatalogPath

        if not self.results:
            return CF.create_empty()

        # Check result type by name to avoid circular imports
        first = self.results[0]
        type_name = type(first).__name__

        if type_name == "TableResult":
            rows = []
            for r in self.results:
                rows.append(
                    {
                        "table": getattr(r, "table", ""),
                        "dataset": getattr(r, "dataset", ""),
                        "version": getattr(r, "version", ""),
                        "namespace": getattr(r, "namespace", ""),
                        "channel": getattr(r, "channel", ""),
                        "path": getattr(r, "path", ""),
                        "is_public": getattr(r, "is_public", True),
                        "dimensions": getattr(r, "dimensions", []),
                        "format": getattr(r, "formats", ["feather"])[0] if getattr(r, "formats", []) else "feather",
                    }
                )
            frame = CF(rows)
            frame._base_uri = self.base_url or OWID_CATALOG_URI
            return frame

        elif type_name == "IndicatorResult":
            rows = []
            for r in self.results:
                path = getattr(r, "path", None)
                # Parse catalog path using CatalogPath
                try:
                    if path is None:
                        raise ValueError("path is None")
                    parsed = CatalogPath.from_str(path)
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
                    path_part = path.split("#")[0] if path and "#" in path else path

                rows.append(
                    {
                        "indicator_title": getattr(r, "title", ""),
                        "indicator": indicator,
                        "score": getattr(r, "score", 0.0),
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
            frame._base_uri = self.base_url or OWID_CATALOG_URI
            return frame

        else:
            raise TypeError(f"Cannot convert {type_name} results to CatalogFrame")

    def filter(self, predicate: Callable[[T], bool]) -> "ResponseSet[T]":
        """Filter results by predicate function.

        Returns a new ResponseSet with only items that match the predicate.
        The predicate should return True for items to keep.

        Args:
            predicate: Function that takes an item of results (e.g. ChartResult) and returns True/False.

        Returns:
            New ResponseSet with filtered results.

        Example:
            ```py
            >>> # Filter results by version
            >>> results.filter(lambda r: r.version > '2024')

            >>> # Filter by namespace
            >>> results.filter(lambda r: r.namespace == "worldbank")

            >>> # Chain multiple filters
            >>> results.filter(lambda r: r.version > '2024').filter(lambda r: r.namespace == "un")
            ```
        """
        filtered_results = [item for item in self.results if predicate(item)]
        return ResponseSet(
            results=filtered_results,
            query=self.query,
            total_count=len(filtered_results),
            base_url=self.base_url,
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
            ```py
            >>> # Sort by version (ascending)
            >>> results.sort_by('version')

            >>> # Sort by version (descending - latest first)
            >>> results.sort_by('version', reverse=True)

            >>> # Sort by custom function (e.g., by score)
            >>> results.sort_by(lambda r: r.score, reverse=True)

            >>> # Chain sorting and filtering
            >>> results.filter(lambda r: r.version > '2024').sort_by('version', reverse=True)
            ```
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
            total_count=self.total_count,
            base_url=self.base_url,
        )

    def _get_version_string(self, item: T) -> str:
        """Get a sortable version string for any result type.

        Returns a string that can be used for chronological sorting:
        - ChartResult: ISO format string from last_updated (with time)
        - TableResult/IndicatorResult: version string

        This allows consistent sorting across different result types,
        even in mixed-type ResponseSets.
        """
        type_name = type(item).__name__

        if type_name == "ChartResult":
            last_updated = getattr(item, "last_updated", None)
            if last_updated:
                return last_updated.isoformat()
            return ""
        else:
            # TableResult, IndicatorResult - use version
            version = getattr(item, "version", None)
            if version:
                return str(version)
            return ""

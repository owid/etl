#
#  owid.catalog.client.models
#
#  Pydantic model definitions for API responses.
#
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Generic, Iterator, TypeVar
from urllib import parse

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

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

    # Tweak this to have a more advanced display
    _ui_advanced: bool = False

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

        # For ChartResult, add thumbnail column and make URL clickable
        if self.results and type(self.results[0]).__name__ == "ChartResult" and "url" in df.columns:
            df = df.copy()

            # Add thumbnail column - clickable to open chart
            df.insert(
                0,
                "preview",
                df["url"].apply(
                    lambda x: f'<a href="{x}" target="_blank"><img style="max-height:150px; border-radius:4px;" src="{get_thumbnail_url(x)}"></a>'
                    if x
                    else ""
                ),
            )

            # Make URL a clickable link
            df["url"] = df["url"].apply(lambda x: f'<a href="{x}" target="_blank">{x.split("/")[-1]}</a>' if x else "")

        # Use pandas Styler for left-alignment on all result types
        styler = df.style.set_table_styles(
            [
                {"selector": "td, th", "props": [("text-align", "left")]},
            ]
        )

        df_html = styler.to_html(escape=False)

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
                        "description": getattr(r, "description", ""),
                        "url": getattr(r, "url", ""),
                        "num_related_articles": getattr(r, "num_related_articles", 0),
                        # Only show count of entities, not full list
                        "num_entities": len(getattr(r, "available_entities", [])),
                        "popularity": getattr(r, "popularity", 0),
                        "last_updated": getattr(r, "last_updated", None),
                    }

                    # Simplify if not advanced UI
                    if not self._ui_advanced:
                        row = {
                            # "slug": row["slug"],
                            "title": row["title"],
                            "description": row["description"],
                            "last_updated": row["last_updated"],
                            "url": row["url"],
                        }
                else:
                    row = r.model_dump()

                    # Simplify if not advanced UI
                    if not self._ui_advanced:
                        row = {
                            "title": row.get("title") or "",
                            "description": row.get("description") or "",
                            "version": row.get("version") or "",
                            "path": row.get("path") or "",
                        }
                rows.append(row)
            else:
                rows.append(r)

        return pd.DataFrame(rows)

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
            _ui_advanced=self._ui_advanced,
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
            _ui_advanced=self._ui_advanced,
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


def get_thumbnail_url(grapher_url: str) -> str:
    """
    Turn https://ourworldindata.org/grapher/life-expectancy?country=~CHN"
    Into https://ourworldindata.org/grapher/thumbnail/life-expectancy.png?country=~CHN
    """
    parts = parse.urlparse(grapher_url)

    return f"{parts.scheme}://{parts.netloc}/grapher/thumbnail/{Path(parts.path).name}.png?{parts.query}"

#
#  owid.catalog.client.models
#
#  Pydantic model definitions for API responses.
#
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Generic, Iterator, TypeVar, overload
from urllib import parse

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")


class ResponseSet(BaseModel, Generic[T]):
    """Generic container for API responses.

    Provides iteration, indexing, and conversion to CatalogFrame
    for backwards compatibility.

    Attributes:
        items: List of result objects.
        query: The query that produced these results.
        total_count: Total number of results available (may be more than len(items)).
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    items: list[T]
    query: str = ""
    total_count: int = 0
    base_url: str = Field(frozen=True)

    # Tweak this to have a more advanced display
    _ui_advanced: bool = False

    def _get_type_display(self) -> str:
        """Get display name for ResponseSet with generic type."""
        if not self.items:
            return "ResponseSet"

        # Get the type of the first result
        first_result = self.items[0]
        type_name = type(first_result).__name__
        return f"ResponseSet[{type_name}]"

    def model_post_init(self, __context: Any) -> None:
        """Set total_count to length of results if not provided."""
        if self.total_count == 0:
            self.total_count = len(self.items)

    def __iter__(self) -> Iterator[T]:  # type: ignore[override]
        """Iterate over results, not model fields."""
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> "ResponseSet[T]": ...

    def __getitem__(self, index: int | slice) -> "T | ResponseSet[T]":
        if isinstance(index, slice):
            return ResponseSet(
                items=self.items[index],
                query=self.query,
                total_count=len(self.items[index]),
                base_url=self.base_url,
                _ui_advanced=self._ui_advanced,
            )
        return self.items[index]

    # Display settings
    _MAX_DISPLAY_ROWS: int = 60
    _HEAD_ROWS: int = 5
    _TAIL_ROWS: int = 5
    _MAX_STR_LENGTH: int = 80

    def _truncate_string(self, val: Any, max_len: int) -> Any:
        """Truncate string values that exceed max length. Skip HTML content."""
        if isinstance(val, str):
            # Skip HTML content (contains tags)
            if "<" in val and ">" in val:
                return val
            if len(val) > max_len:
                return val[: max_len - 3] + "..."
        return val

    def _truncate_strings_in_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Truncate all string values in DataFrame for display."""
        df = df.copy()
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].apply(lambda x: self._truncate_string(x, self._MAX_STR_LENGTH))
        return df

    def _get_display_frame(self) -> tuple[pd.DataFrame, bool]:
        """Get DataFrame for display, truncated if needed.

        Returns:
            Tuple of (display DataFrame, whether it was truncated).
        """
        df = self.to_frame()

        if len(df) <= self._MAX_DISPLAY_ROWS:
            return df, False

        # Truncate: show head and tail
        head = df.head(self._HEAD_ROWS)
        tail = df.tail(self._TAIL_ROWS)
        return pd.concat([head, tail]), True

    def __repr__(self) -> str:
        """Display results as a formatted table for better readability."""
        type_display = self._get_type_display()

        if not self.items:
            return f"{type_display}(query={self.query!r}, total_count=0, items=[])"

        df, truncated = self._get_display_frame()

        if len(df) == 0:
            return f"{type_display}(query={self.query!r}, total_count={self.total_count}, items=[])"

        df_str = str(df)

        # Insert ellipsis row if truncated
        if truncated:
            lines = df_str.split("\n")
            # Find where to insert "..." (after header + HEAD_ROWS data rows)
            # Header is first line, then HEAD_ROWS of data
            insert_pos = 1 + self._HEAD_ROWS
            ellipsis_line = "..." + " " * (len(lines[0]) - 3) if lines else "..."
            lines.insert(insert_pos, ellipsis_line)
            df_str = "\n".join(lines)

        # Format as bullet points to show attributes at same level
        # Indent DataFrame lines to align with bullet points
        df_lines = df_str.split("\n")
        indented_df = "\n    ".join(df_lines)

        header = f"{type_display}\n.query={self.query!r}\n.total_count={self.total_count}\n.items:\n    {indented_df}"

        # Add helper tip at the end
        tip = "\n\nTip: Use .to_frame() for pandas, .to_dict() for plain dicts, or .latest(by='field') for most recent"
        return header + tip

    def __str__(self) -> str:
        """Use the same representation for str() and repr()."""
        return self.__repr__()

    def _repr_html_(self) -> str:
        """Display as HTML table in Jupyter notebooks."""
        type_display = self._get_type_display()

        if not self.items:
            return f"<p>{type_display}(query={self.query!r}, limit=0, items=[])</p>"

        df, truncated = self._get_display_frame()

        # For ChartResult, add thumbnail column and make URL clickable
        if self.items and type(self.items[0]).__name__ == "ChartResult" and "url" in df.columns:
            df = df.copy()  # Get display labels from results (slug + query_params for explorers/multidim)

            def _slug_label(r: Any) -> str:
                slug = getattr(r, "slug", "")
                query_params = getattr(r, "query_params", "")
                if query_params:
                    return f"{slug}{query_params}"
                return slug

            slugs = [_slug_label(r) for r in self.items]
            # Handle truncated display (head + tail)
            if truncated:
                slugs = slugs[: self._HEAD_ROWS] + slugs[-self._TAIL_ROWS :]

            # Add thumbnail column - clickable to open chart
            preview_col = pd.Series(
                [
                    f'<a href="{x}" target="_blank"><img style="max-height:150px; border-radius:4px;" src="{get_thumbnail_url(x)}"></a>'
                    if x
                    else ""
                    for x in df["url"]
                ]
            )
            df.insert(0, "preview", preview_col)

            # Make URL a clickable link using slug from results
            df["url"] = [
                f'<a href="{url}" target="_blank">{slug}</a>' if url else "" for url, slug in zip(df["url"], slugs)
            ]

        # Insert ellipsis row if truncated
        if truncated:
            df = df.reset_index(drop=True)
            # Create ellipsis row
            ellipsis_row = pd.DataFrame([{col: "..." for col in df.columns}])
            # Split at HEAD_ROWS and insert ellipsis
            head = df.iloc[: self._HEAD_ROWS]
            tail = df.iloc[self._HEAD_ROWS :]
            df = pd.concat([head, ellipsis_row, tail], ignore_index=True)

        # Truncate long strings for display (skips HTML content)
        df = self._truncate_strings_in_df(df)

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
    <li><strong>.items</strong>:
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
        if not self.items:
            raise ValueError("No results available to get latest from")

        # Auto-detect sort key based on result type
        if by is None:
            return max(self.items, key=self._get_version_string)

        # Explicit attribute name
        if not hasattr(self.items[0], by):
            # Get available attributes (exclude private ones)
            available = [
                k for k in dir(self.items[0]) if not k.startswith("_") and not callable(getattr(self.items[0], k))
            ]
            raise AttributeError(
                f"Results don't have '{by}' attribute. " f"Available attributes: {', '.join(sorted(available))}"
            )

        return max(self.items, key=lambda item: getattr(item, by))

    def to_frame(self, advanced: bool | None = None) -> pd.DataFrame:
        """Convert results to a DataFrame.

        Args:
            advanced: If True, show all fields. If False, show only key fields.
                If None (default), use the instance's _ui_advanced setting.

        Returns:
            DataFrame with one row per result.
        """
        if not self.items:
            return pd.DataFrame()

        # Resolve effective advanced flag: explicit arg > instance setting
        is_advanced = advanced if advanced is not None else self._ui_advanced

        # Convert Pydantic models to dicts
        rows = []
        for r in self.items:
            if isinstance(r, BaseModel):
                # For ChartResult, exclude large dict fields for better display
                # Use type name check to avoid circular imports
                if type(r).__name__ == "ChartResult":
                    row = {
                        "type": getattr(r, "type", ""),
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
                    if not is_advanced:
                        row = {
                            "title": row["title"],
                            "description": row["description"],
                            "last_updated": row["last_updated"],
                            "url": row["url"],
                        }
                else:
                    row = r.model_dump()

                    # Exclude internal config fields that aren't useful to display
                    row.pop("catalog_url", None)
                    row.pop("base_url", None)

                    # Simplify if not advanced UI
                    if not is_advanced:
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

    def to_dict(self) -> list[dict[str, Any]]:
        """Convert results to a list of plain dictionaries.

        Useful for serializing results for AI/LLM context windows
        or any scenario where you need simple dict representations.

        Returns:
            List of dictionaries, one per result item.

        Example:
            ```py
            >>> results = client.charts.search("gdp")
            >>> results.to_dict()
            [{'slug': 'gdp-per-capita', 'title': 'GDP per capita', ...}, ...]
            ```
        """
        if not self.items:
            return []

        if isinstance(self.items[0], BaseModel):
            return [item.model_dump() for item in self.items]  # type: ignore[union-attr]

        return list(self.items)  # type: ignore[arg-type]

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
        filtered_results = [item for item in self.items if predicate(item)]
        return ResponseSet(
            items=filtered_results,
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
            sorted_results = sorted(self.items, key=lambda item: getattr(item, key), reverse=reverse)
        else:
            # Sort by key function
            sorted_results = sorted(self.items, key=key, reverse=reverse)

        return ResponseSet(
            items=sorted_results,
            query=self.query,
            total_count=self.total_count,
            base_url=self.base_url,
            _ui_advanced=self._ui_advanced,
        )

    def set_ui_advanced(self) -> "ResponseSet[T]":
        """Switch to advanced display showing all fields (type, slug, popularity, etc.).

        Returns:
            Self (for chaining).

        Example:
            ```py
            >>> results.set_ui_advanced()
            ```
        """
        self._ui_advanced = True
        return self

    def set_ui_basic(self) -> "ResponseSet[T]":
        """Switch to basic display showing only key fields (title, description, url).

        Returns:
            Self (for chaining).

        Example:
            ```py
            >>> results.set_ui_basic()
            ```
        """
        self._ui_advanced = False
        return self

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


def get_thumbnail_url(url: str) -> str:
    """
    Turn https://ourworldindata.org/grapher/life-expectancy?country=~CHN"
    Into https://ourworldindata.org/grapher/life-expectancy.png?country=~CHN
    """
    parts = parse.urlparse(url)
    if "/explorers/" in url:
        url = f"{parts.scheme}://{parts.netloc}/explorers/{Path(parts.path).name}.png?{parts.query}"
    else:
        url = f"{parts.scheme}://{parts.netloc}/grapher/{Path(parts.path).name}.png?{parts.query}"
    return url

#
#  owid.catalog.core.charts
#
#  Chart-specific data structures for OWID chart data.
#
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from owid.catalog.meta import TableMeta
from owid.catalog.tables import Table


@dataclass
class ChartTableMeta(TableMeta):
    """Metadata for chart tables.

    Extends TableMeta for chart-specific data. Charts don't belong to datasets
    in the traditional ETL sense, so dataset-related fields return None.

    Attributes:
        short_name: The chart slug (e.g., "life-expectancy").
        title: Chart title from config.
        description: Chart subtitle from config.
    """

    # Charts don't have a dataset reference
    dataset: None = field(default=None, compare=False)  # type: ignore[assignment]

    @property
    def uri(self) -> str | None:
        """Return chart URL or None if short_name not set.

        For charts, the URI is the full grapher URL.
        """
        if not self.short_name:
            return None
        return f"https://ourworldindata.org/grapher/{self.short_name}"


class ChartTable(Table):
    """A Table subclass for chart data with chart configuration.

    Extends Table to include chart-specific configuration from OWID charts.
    The chart_config dictionary contains the full grapher configuration.

    Attributes:
        chart_config: Chart configuration dictionary from the grapher.
        metadata: ChartTableMeta instance with chart-specific metadata.

    Example:
        ```python
        tb = client.charts.fetch("life-expectancy")
        print(tb.chart_config.get("title"))  # Chart title
        print(tb.chart_config.get("subtitle"))  # Chart subtitle
        print(tb.metadata.uri)  # https://ourworldindata.org/grapher/life-expectancy
        ```
    """

    # Register chart_config as metadata to propagate through pandas operations
    _metadata = Table._metadata + ["_chart_config"]

    def __init__(self, *args: Any, chart_config: dict[str, Any] | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._chart_config = chart_config or {}

    @property
    def chart_config(self) -> dict[str, Any]:
        """Chart configuration dictionary."""
        return self._chart_config

    @chart_config.setter
    def chart_config(self, value: dict[str, Any]) -> None:
        self._chart_config = value

    @property
    def _constructor(self) -> type["ChartTable"]:
        """Return ChartTable for pandas operations that return new instances."""
        return ChartTable

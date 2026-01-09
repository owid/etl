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
        chart_config: Chart configuration dictionary from the grapher.
    """

    # Charts don't have a dataset reference
    dataset: None = field(default=None, compare=False)  # type: ignore[assignment]

    # Store chart configuration in metadata
    chart_config: dict[str, Any] = field(default_factory=dict)

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
    The chart_config dictionary is stored in metadata and contains the full
    grapher configuration.

    Attributes:
        metadata: ChartTableMeta instance with chart-specific metadata
            (including chart_config).

    Example:
        ```python
        tb = client.charts.fetch("life-expectancy")
        print(tb.metadata.chart_config.get("title"))  # Chart title
        print(tb.metadata.chart_config.get("subtitle"))  # Chart subtitle
        print(tb.metadata.uri)  # https://ourworldindata.org/grapher/life-expectancy
        ```
    """

    # Explicitly define metadata type
    metadata: ChartTableMeta

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # Ensure we use ChartTableMeta instead of TableMeta
        if "metadata" not in kwargs or kwargs["metadata"] is None:
            # No metadata provided - create empty ChartTableMeta
            kwargs["metadata"] = ChartTableMeta()

        super().__init__(*args, **kwargs)

        # After super().__init__, convert TableMeta to ChartTableMeta if needed
        # This handles pandas operations that may have set a TableMeta
        if not isinstance(self.metadata, ChartTableMeta):
            self.metadata = ChartTableMeta(
                **{k: v for k, v in self.metadata.__dict__.items() if k in self.metadata.__dataclass_fields__}
            )

    @property
    def _constructor(self) -> type["ChartTable"]:
        """Return ChartTable for pandas operations that return new instances."""
        return ChartTable

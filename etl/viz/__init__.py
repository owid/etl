"""Visualizations built from ETL indicators: charts (single charts and MDIMs), explorers and static
images. Each kind has a `viz://` step channel, with recipes under `etl/steps/viz/<channel>/`.

The names most steps need are re-exported here; the submodules hold the rest:

- `etl.viz.chart`: the chart model (`Chart`, `View`, `Dimension`) and the functions that build one
  from a config and dimensional tables (`create_chart`, `combine_charts`, `expand_config`).
- `etl.viz.explorer`: `Explorer` (a `Chart` subclass), the legacy TSV explorer, and the CSV-explorer migration.
- `etl.viz.static`: the export contract shared by `viz://static` steps.
"""

from etl.viz.chart import (
    Chart,
    ChartSet,
    combine_charts,
    combine_config_dimensions,
    create_chart,
    expand_config,
    filter_columns_by_dimension_choices,
)
from etl.viz.explorer import Explorer

__all__ = [
    "Chart",
    "ChartSet",
    "Explorer",
    "combine_charts",
    "combine_config_dimensions",
    "create_chart",
    "expand_config",
    "filter_columns_by_dimension_choices",
]

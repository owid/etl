"""A ChartSet holds the Chart objects a viz step saved under `viz/<channel>/...`, read back by name.

Works similarly to a Dataset (from owid.catalog), which holds Tables.
"""

from pathlib import Path

from etl.viz.chart.model.core import Chart
from etl.viz.explorer.core import Explorer


class ChartSet:
    def __init__(self, path: Path):
        self.path = path
        self.charts = self._build_dictionary()
        # Pick the concrete subclass to instantiate. The viz directory layout
        # encodes the chart type: `viz/explorer/...` → Explorer,
        # `viz/chart/...` → Chart. Without this dispatch,
        # `Chart.load` on an Explorer config raises on the missing `title`
        # field (which Explorer.from_dict defaults to `{}`).
        self._cls: type[Chart] = Explorer if "explorer" in path.parts else Chart

    def _build_dictionary(self) -> dict[str, Path]:
        dix = {}
        paths = self.path.glob(r"*.config.json")
        for p in paths:
            name = p.name.replace(".config.json", "")
            dix[name] = p
        return dix

    def read(self, name: str) -> Chart:
        # Check if chart exists
        if name not in self.charts:
            raise ValueError(
                f"Chart name not available. Available options are {self.names}. If this does not make sense to you, try running the necessary steps to re-export files to {self.path}"
            )

        # Read MDIM
        path = self.charts[name]
        try:
            c = self._cls.load(str(path))
        except TypeError as e:
            # This is a workaround for the TypeError that occurs when loading the config file.
            raise TypeError(
                f"Error loading Chart config file. Please check the file format and ensure it is valid JSON. Suggestion: Re-run the viz step generating {name}. Error: {e}"
            )

        # Get and set catalog path
        return c

    @property
    def names(self):
        return list(sorted(self.charts.keys()))

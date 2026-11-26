"""COVID-19 data explorer, as a multidim. It replaces the `viz://explorer/covid/latest/covid` explorer."""

from etl.helpers import PathFinder
from etl.viz.chart.explorer_text import add_explorer_text

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    c = paths.create_chart(config=paths.load_config())

    # The explorer it replaces has a download button; keep pointing at the same page (covid.explorer.md)
    # until grapher links to the multidim's own download package (see etl/viz/chart/explorer_text.py).
    add_explorer_text(c, paths)

    # The variants view sorts its stacked bars by the total Omicron share, which it doesn't plot.
    c.save(tolerate_extra_indicators=True)

"""Multidim built from its .config.yml; a .py to add the explorer's text and skip the download package (see the NOTEs)."""

from etl.helpers import PathFinder
from etl.viz.chart.explorer_text import add_explorer_text

paths = PathFinder(__file__)


def run() -> None:
    chart = paths.create_chart(config=paths.load_config())
    # NOTE: the text below the explorer lives in influenza.explorer.md, appended to every view's key points
    # until grapher can show multidim-level text (see etl/viz/chart/explorer_text.py).
    add_explorer_text(chart, paths)
    # NOTE: no download package until it handles weekly and monthly indicators
    # that share a name and dates; building it fails. See owid/etl#7336.
    chart.save(download_package=False)

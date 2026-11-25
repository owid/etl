"""Mpox data explorer, as a multidim. It replaces the `viz://explorer/who/latest/monkeypox` explorer."""

from etl.helpers import PathFinder
from etl.viz.chart.explorer_text import add_explorer_text

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    c = paths.create_chart(config=paths.load_config())

    # The explorer it replaces has a download button; keep pointing at the same file (monkeypox.explorer.md)
    # until grapher links to the multidim's own download package (see etl/viz/chart/explorer_text.py).
    add_explorer_text(c, paths)

    c.save()

"""COVID-19 data explorer, as a multidim. It replaces the `viz://explorer/covid/latest/covid` explorer."""

from etl.helpers import PathFinder
from etl.viz.chart.download_link import add_download_link

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Interim download link (see etl/viz/chart/download_link.py).
DOWNLOAD_URL = "https://docs.owid.io/projects/etl/api/covid/"
DOWNLOAD_TEXT = "Download our complete COVID-19 dataset [from our data catalog]({url})."


def run() -> None:
    c = paths.create_chart(config=paths.load_config())

    # The explorer it replaces has a download button; keep pointing at the same page until grapher
    # links to the multidim's own download package.
    add_download_link(c, url=DOWNLOAD_URL, text=DOWNLOAD_TEXT)

    c.save()

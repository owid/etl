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

    # The variants view sorts its stacked bars by the total Omicron share, which it doesn't plot.
    # NOTE: No download package until the date-axis indicators declare `display.timeInterval`
    # (several of them don't, so the package can't label their rows with a frequency). Readers
    # get the complete dataset from the link above meanwhile.
    c.save(tolerate_extra_indicators=True, download_package=False)

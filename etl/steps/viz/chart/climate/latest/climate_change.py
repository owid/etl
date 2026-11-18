"""Multidim built from its .config.yml; a .py only to skip the download package (see the NOTE)."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    chart = paths.create_chart(config=paths.load_config())
    # NOTE: no download package until it handles a table that mixes daily,
    # monthly and quarterly columns; building it fails. See owid/etl#7336.
    chart.save(download_package=False)

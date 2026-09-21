"""Script to create a snapshot of dataset.

The data is a five-country extract of the indicators "Employment in
agriculture/industry/services (% of total employment)" from an archived edition of the
World Development Indicators: the earliest values available around 1980 for France,
Italy and the United Kingdom (1980) and for the Netherlands and Poland (1981, with 1980
unavailable). All values were verified against the April 2013 edition in the World
Bank's WDI Database Archives (databank source 57); the same values are also preserved in
the data files of Herrendorf, Rogerson and Valentinyi (2014) (WDISectorData.xls in the
archive snapshotted as papers/2026-07-02/herrendorf_rogerson_valentinyi.zip). These
values connect the pre-1800 benchmark estimates of Broadberry and Gardner (2013) with
the modern ILO-modeled series that begins in 1991.

To create the snapshot, run:
    etls wb/2026-07-02/wdi_employment_by_sector_archive --path-to-file <path>
"""

import click

from etl.helpers import PathFinder

paths = PathFinder(__file__)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def run(upload: bool = True, path_to_file: str = "") -> None:
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)

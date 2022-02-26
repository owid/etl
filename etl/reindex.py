#
#  reindex.py
#  etl
#

import click
from pathlib import Path

from owid.catalog import LocalCatalog

from etl.paths import DATA_DIR


@click.command()
def reindex() -> None:
    LocalCatalog(Path(DATA_DIR)).reindex()


if __name__ == "__main__":
    reindex()

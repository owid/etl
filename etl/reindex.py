#
#  reindex.py
#  etl
#

import click

from owid.catalog import LocalCatalog

from etl.paths import DATA_DIR


@click.command()
def reindex() -> None:
    LocalCatalog(DATA_DIR).reindex()


if __name__ == "__main__":
    reindex()

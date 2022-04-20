#
#  reindex.py
#  etl
#

import click
from pathlib import Path

from owid.catalog import LocalCatalog, CHANNEL

from etl.paths import DATA_DIR


@click.command()
@click.option(
    "--channel",
    "-c",
    multiple=True,
    type=click.Choice(CHANNEL.__args__),
    help="Reindex only selected channel (subfolder of data/), reindex all by default",
)
def reindex(channel: tuple[CHANNEL]) -> None:
    LocalCatalog(Path(DATA_DIR), channels=channel).reindex()


if __name__ == "__main__":
    reindex()

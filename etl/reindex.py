#
#  reindex.py
#  etl
#

import click
from pathlib import Path
from collections.abc import Iterable

from owid.catalog import LocalCatalog, CHANNEL

from etl.paths import DATA_DIR


@click.command()
@click.option(
    "--channel",
    "-c",
    multiple=True,
    type=click.Choice(CHANNEL.__args__),
    default=CHANNEL.__args__,
    help="Reindex only selected channel (subfolder of data/), reindex all by default",
)
def reindex_cli(channel: Iterable[CHANNEL]) -> None:
    return reindex(channel=channel)


def reindex(channel: Iterable[CHANNEL]) -> None:
    LocalCatalog(Path(DATA_DIR), channels=channel).reindex()


if __name__ == "__main__":
    reindex_cli()

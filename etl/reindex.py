#
#  reindex.py
#  etl
#

import click
from pathlib import Path
from collections.abc import Iterable
from typing import Optional

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
@click.option(
    "--include",
    type=str,
    help="Reindex only datasets matching pattern",
)
def reindex_cli(channel: Iterable[CHANNEL], include: Optional[str]) -> None:
    return reindex(channel=channel, include=include)


def reindex(channel: Iterable[CHANNEL], include: Optional[str] = None) -> None:
    LocalCatalog(Path(DATA_DIR), channels=channel).reindex(include=include)


if __name__ == "__main__":
    reindex_cli()

#
#  reindex.py
#  etl
#

from collections.abc import Iterable
from pathlib import Path
from typing import Optional

import click
from owid.catalog import CHANNEL, LocalCatalog

from etl import config
from etl.paths import DATA_DIR

config.enable_bugsnag()


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

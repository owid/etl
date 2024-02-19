#
#  reindex.py
#  etl
#

from collections.abc import Iterable
from pathlib import Path
from typing import Optional

import rich_click as click
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
    help="Reindex only the selected channel(s) (subfolders of data/)",
)
@click.option(
    "--include",
    type=str,
    help="Reindex only datasets matching pattern",
)
def reindex_cli(channel: Iterable[CHANNEL], include: Optional[str]) -> None:
    """Create a catalog-[channel].feather file inside etl/data with all tables in each channel.

    # Description
    This enables `catalog.find` to be aware of what datasets currently exists. So, if for example you create a new dataset locally, you won't be able to find it in your local catalog unless you re-run reindex.

    # Reference
    """
    return reindex(channel=channel, include=include)


def reindex(channel: Iterable[CHANNEL], include: Optional[str] = None) -> None:
    LocalCatalog(Path(DATA_DIR), channels=channel).reindex(include=include)


if __name__ == "__main__":
    reindex_cli()

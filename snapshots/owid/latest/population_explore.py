from pathlib import Path

import click
from structlog import get_logger

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Log
log = get_logger()


########################################################################################################################
# TODO: Temporarily using a local file until 2024 revision is released
#  The download url should still be the same:
#  https://population.un.org/wpp
########################################################################################################################
@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", type=str, help="Path to population local file.")
def main(
    upload: bool,
    path_to_file: str | None = None,
) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"owid/{SNAPSHOT_VERSION}/population_explore.xlsx")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()

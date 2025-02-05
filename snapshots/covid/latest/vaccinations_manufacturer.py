"""
The data has been obtained from covid-19-data repository.

Run the lines from https://github.com/owid/covid-19-data/blob/d8fae5631e2130270532951400fa449a9b20d00b/scripts/src/cowidev/cmd/vax/generate/utils.py#L712-L734, and export df_manufacturer.
"""

from pathlib import Path
from typing import Optional

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(upload: bool, path_to_file: Optional[str] = None) -> None:
    # MANUAL
    ## This is a snapshot from github.com/owid/covid-19-data as of 15th August 2024
    ## No need to re-run it again
    if path_to_file:
        snap = Snapshot(f"covid/{SNAPSHOT_VERSION}/vaccinations_manufacturer.csv")
        snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()

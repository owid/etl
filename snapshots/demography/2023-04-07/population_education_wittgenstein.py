"""Script to create a snapshot of dataset 'Population size by education level (Wittgenstein Centre, 2018)'.

The file used in here is downloaded manually from Wittgenstein's site:

- Visit http://dataexplorer.wittgensteincentre.org/wcde-v2/
- In 2. Geography, select "World" as the Region, and then check the box "Include countries of selected regions".
- In 4. Time Horizon, choose "Medium (SSP2)" as the Scenario, and then check the box "Include all times".
- Click on "Download data".

"""

from pathlib import Path
from typing import Union

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
@click.option("--path-to-file", prompt=True, type=str, help="Path to local file.")
def main(path_to_file: Union[Path, str], upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"demography/{SNAPSHOT_VERSION}/population_education_wittgenstein.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()

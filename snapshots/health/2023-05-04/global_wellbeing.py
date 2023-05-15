"""Script to create a snapshot of dataset 'Global Wellbeing Initiative (Gallup, 2020)'.

The dataset file is downloaded manually from the source and saved to the local. Follow the steps:

- Visit https://www.gallup.com/analytics/468179/global-wellbeing-initiative-dataset.aspx
- Fill the form at the bottom of the page (need to enter mail, phone number)
- Download the dataset file
- Run script as `python snapshots/health/2023-05-04/gallup.py --path-to-file <path-to-file>`
"""

from pathlib import Path

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
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"health/{SNAPSHOT_VERSION}/global_wellbeing.xlsx")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()

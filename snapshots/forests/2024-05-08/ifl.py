"""Script to create a snapshot of dataset.

Data was sent by email from the source - Peter Potapov (potapov@umd.edu) - on 2024-05-07 is the country level aggregates of the Intact Forest Landscapes data produced by Potapov et al (https://intactforests.org/)
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"forests/{SNAPSHOT_VERSION}/ifl.xlsx")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()

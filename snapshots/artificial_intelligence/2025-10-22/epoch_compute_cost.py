"""Script to create a snapshot of the Epoch AI training cost dataset.

The dataset was provided directly by the authors in a private communication.
For the latest version, see the public GitHub repository:
https://github.com/epoch-research/training-cost-trends

To update:
1. Clone the repository
2. Run the analysis to generate the latest data
3. Run this script with the path to the generated CSV file
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file")
def main(path_to_file: str, upload: bool) -> None:
    """Create a new snapshot of the Epoch AI training cost dataset."""
    # Create a new snapshot.
    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/epoch_compute_cost.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()

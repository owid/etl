"""Script to create a snapshot of dataset.

To access the data for this snapshot, follow these steps:

- Go to https://vizhub.healthdata.org/microbe/
- Click on Antimicrobial Resistance in the top tab
- Select these options:
    - Category: Syndromes
    - Burden: By resistance
    - Location: Global
    - Age: Neonatal
    - Counterfactual: Both (or Attributable)
    - Year: 2021 (or whatever latest year is available)
    - Measure: Deaths
    - Metric: Number

Download the file and upload it using the script below.


"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", "-f", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"antibiotics/{SNAPSHOT_VERSION}/microbe_amr.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()

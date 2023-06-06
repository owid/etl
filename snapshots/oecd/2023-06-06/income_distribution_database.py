"""Script to create a snapshot of dataset 'Income Distribution Database - Inequality (OECD, 2022)'.
Steps to follow for an update:
1. Go to this saved query on OECD.Stat: https://stats.oecd.org//Index.aspx?QueryId=123861
2. Make sure to get the full dataset: Go to Customise > Selection and
    - Select all countries.
    - Make sure all the inequality measures are selected.
    - Make sure "Total population" is selected as age group.
    - Make sure "Current definition" is selected as definition.
    - Make sure "New income definition since 2012" is selected as methodology.
    - Go to Year > Select time period > Select all.
    - Press "View Data"
3. Export the data as CSV, by selecting Export > Text file (CSV) > Default format > Download.
4. Save the file in the same folder as this script.
5. Run this script with the path to the file as argument:
    python snapshots/oecd/2023-06-06/income_distribution_database.py --path-to-file <path-to-file>

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
    snap = Snapshot(f"oecd/{SNAPSHOT_VERSION}/income_distribution_database.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()

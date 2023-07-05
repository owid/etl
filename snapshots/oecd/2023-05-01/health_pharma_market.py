"""Script to create a snapshot of dataset 'Health: Pharmaceutical Market (OECD, 2022)'.

This is a subset of all OECD data. Only concerns Health data, and within Health the "Pharmaceutical Market".

To obtain the file locally follow these steps:

- Go to https://stats.oecd.org/Index.aspx?DataSetCode=HEALTH_PHMC
- You will see the table with the data. Click on "Customize" on the upper right side of the table
- Click on layout.
- Move and drop "Variable" and "Year" to the Row box.
- Click on "View Data"
- Then from the same menu, click "Export", and then "Text file (CSV)"
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
    snap = Snapshot(f"oecd/{SNAPSHOT_VERSION}/health_pharma_market.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()

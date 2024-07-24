"""Script to create a snapshot of dataset.

Steps to download this data:

- Navigate to the following URL: https://polioeradication.org/financing/donors/historical-contributions/
- On the first tab 'GPEI Contributions' from 1985 to 2022
- Scroll down within the Tableau pane
- Click on the 'Download' icon (third from right)
- Select 'Crosstab'
- Select the format as 'Excel' and Download (the csv format is messed up)

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
    snap = Snapshot(f"health/{SNAPSHOT_VERSION}/gpei_funding.xlsx")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()

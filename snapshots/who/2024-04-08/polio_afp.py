"""Script to create a snapshot of dataset.

To find the data needed to run this step following these steps:

 - Go to https://extranet.who.int/polis/public/CaseCount.aspx
 - Select 'World' in the Region list
 - Select all countries in the year of onset list (you may need to use cmd+a to do this)
 - Ensure the 'Country Detail' box is checked
 - Click 'Show data'
 - Select the outputted table and copy it to a CSV file, e.g. in excel
 - This is the local file to be loaded in the snapshot

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
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/polio_afp.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()

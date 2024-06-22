"""
Script to create a snapshot of the metadata related to Education indicators dataset provided via World Bank.

To create a csv with education related indicators and download it:
    - go to this wesbite http://databank.worldbank.org/Data/Views/VariableSelection/SelectVariables.aspx?source=Education%20Statistics
    - on the left click on Country -> select World, series -> select All and Time -> select All  -->  'Click Download Options' -> Metadata"
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
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/edstats_metadata.xls")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()

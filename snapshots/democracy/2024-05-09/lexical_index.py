"""Script to create a snapshot of dataset.

To download, visit https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/WPKNIT&version=3.0, and download LIED_6.5.xlsx file.

NOTE: in case this site, please look for an alternative from the provider's main site: https://ps.au.dk/en/research/research-projects/dedere/datasets (also listed in the metadata)"""

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
    snap = Snapshot(f"democracy/{SNAPSHOT_VERSION}/lexical_index.xlsx")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()

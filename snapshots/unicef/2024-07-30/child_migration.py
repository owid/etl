"""Script to create a snapshot of dataset.

To import the data set you first need to download the data from the unicef website and save it to a local file. It is the Migration data set on the UNICEF data warehouse
1. Go to https://data.unicef.org/resources/data_explorer/unicef_f/?ag=UNICEF&df=MG&ver=1.0&dq=.MG_INTNL_MG_CNTRY_DEST+MG_RFGS_CNTRY_ASYLM+MG_RFGS_CNTRY_ORIGIN+MG+MG_RFGS_CNTRY_ASYLM_PER1000+MG_RFGS_CNTRY_ASYLM_PER_USD_GNI+MG_INTERNAL_DISP_PERS+MG_NEW_INTERNAL_DISP..&startPeriod=2010&endPeriod=2020
2. Click on the download button (top right) and select  'Full Data in CSV (long format)'."""

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
    snap = Snapshot(f"unicef/{SNAPSHOT_VERSION}/child_migration.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()

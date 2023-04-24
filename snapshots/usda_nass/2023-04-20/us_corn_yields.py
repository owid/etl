"""Script to create a snapshot of dataset 'Long-term corn yields in United States'.

The data was manually downloaded from USDA NASS web site.

To get the data follow these steps (it may take some time, since the web site responds slowly):
* Go to: https://quickstats.nass.usda.gov/
* Select:
    Select Commodity (one or more):
        Program: SURVEY
        Sector: CROPS
        Group: FIELD CROPS
        Commodity: CORN
        Category: YIELD
        Data Item: CORN, GRAIN - YIELD, MEASURED IN BU / ACRE
        Domain: TOTAL
    Select Location (one or more):
        Geographic Level: NATIONAL
        State: US TOTAL
    Select Time (one or more):
        Year: [Select all years]
        Period Type: ANNUAL
        Period: YEAR
* Click on "Get Data" at the bottom left.
* Click on "spreadsheet" at the top right.
* Run this script with the flag `--path-to-file` followed by the path to the downloaded file.

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload snapshot",
)
def main(path_to_file: str, upload: bool) -> None:
    # Create new snapshot.
    snap = Snapshot(f"usda_nass/{SNAPSHOT_VERSION}/us_corn_yields.csv")
    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)
    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())
    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()

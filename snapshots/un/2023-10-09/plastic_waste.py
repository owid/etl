"""Script to create a snapshot of dataset 'UN Comtrade: Plastic Waste Trade (UN, 2023)'.

Data is available from UN Comtrade dashboard: https://comtradeplus.un.org/

Steps to download:

    - Go to 'Preview'
    - Select the following parameters:
        - HS:
            - 3915 (waste, parings and scarp, of plastics)
        - Reporters: All
        - Trade Flows: Import, Export
        - Periods
            - It only allows to select 12 years at once.
            - Select 12 years, download data and then repeat with a different set of 12 years.
        - Partners: All
        - 2nd Partner: World
        - Mode of Transports: TOTAL modes of transport
        - Customs Code: All
        - Breakdown Mode: Plus
        - Aggregate By: None
    - Click on "Download"

As of October 2023, we needed 3 different files, each for a different period:

    - 1987 to 1998
    - 1999 to 2010
    - 2011 to 2022

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
@click.option("--path-to-file-1987_1998", prompt=True, type=str, help="Path to local data file (period 1987 to 1998).")
@click.option("--path-to-file-1999_2010", prompt=True, type=str, help="Path to local data file (period 1999 to 2010).")
@click.option("--path-to-file-2011_2022", prompt=True, type=str, help="Path to local data file (period 2011 to 2022).")
def main(path_to_file_1987_1998: str, path_to_file_1999_2010: str, path_to_file_2011_2022: str, upload: bool) -> None:
    snapshot_paths = [
        f"un/{SNAPSHOT_VERSION}/plastic_waste_1987_1998.csv",
        f"un/{SNAPSHOT_VERSION}/plastic_waste_1999_2010.csv",
        f"un/{SNAPSHOT_VERSION}/plastic_waste_2011_2022.csv",
    ]

    path_to_files = [
        path_to_file_1987_1998,
        path_to_file_1999_2010,
        path_to_file_2011_2022,
    ]
    for meta_path, file_path in zip(snapshot_paths, path_to_files):
        snap = Snapshot(meta_path)
        # Ensure destination folder exists.
        snap.path.parent.mkdir(exist_ok=True, parents=True)
        # Copy local data file to snapshots data folder.
        snap.path.write_bytes(Path(file_path).read_bytes())
        # Add file to DVC and upload to S3.
        snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()

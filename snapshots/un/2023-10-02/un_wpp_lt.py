"""Script to create a snapshot of dataset.

While this ingest script could be done automatically, as of today (2023-10-02) downloading the files from UN WPP is extremely slow. Hence, I've decided to first manually download these and then run the snapshot ingest script.

To download this files:

    1. Go to the CSV Format section of UN WPP page: https://population.un.org/wpp/Download/Standard/CSV/
    2. Download the Life Tables ZIP files with the estimates (1950-2021):
        - https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2022_Life_Table_Complete_Medium_Both_1950-2021.zip
        - https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2022_Life_Table_Complete_Medium_Female_1950-2021.zip
        - https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2022_Life_Table_Complete_Medium_Male_1950-2021.zip
    3. Run the snapshot script and wait for it to be ingested into S3:
        python snapshots/un/2023-10-02/un_wpp_lt.py --path-to-file-all /path/WPP2022_Life_Table_Complete_Medium_Both_1950-2021.zip --path-to-file-f path/WPP2022_Life_Table_Complete_Medium_Female_1950-2021.zip --path-to-file-m path/WPP2022_Life_Table_Complete_Medium_Male_1950-2021.zip

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file-all", prompt=True, type=str, help="Path to local data file (both sexes).")
@click.option("--path-to-file-f", prompt=True, type=str, help="Path to local data file (female).")
@click.option("--path-to-file-m", prompt=True, type=str, help="Path to local data file (male).")
def main(path_to_file_all: str, path_to_file_f: str, path_to_file_m: str, upload: bool) -> None:
    snaps = [
        ("un_wpp_lt_all", path_to_file_all),  # ALL
        ("un_wpp_lt_f", path_to_file_f),  # FEMALE
        ("un_wpp_lt_m", path_to_file_m),  # MALE
    ]

    for snap_props in snaps:
        # Create a new snapshot.
        snap = Snapshot(f"un/{SNAPSHOT_VERSION}/{snap_props[0]}.zip")
        # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
        snap.create_snapshot(filename=snap_props[1], upload=upload)


if __name__ == "__main__":
    main()

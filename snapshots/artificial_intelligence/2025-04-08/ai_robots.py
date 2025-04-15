"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import owid.catalog.processing as pr
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/ai_robots.csv")
    all_dfs = get_data()
    df_to_file(all_dfs, file_path=snap.path)
    # Download data from source, add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def get_data():
    """
    Fetches data from a Google Drive folder for each provided Google Drive ID and concatenates the data into a single DataFrame.

    Returns:
        pd.DataFrame: Concatenated DataFrame containing the fetched data.

    Raises:
        IOError: If there is an error in fetching or concatenating the data.

    """
    common_path = "https://drive.google.com/uc?export=download&id="
    # IDs of the download files (in Google Drive) (Investment by focus area 1 - by country and 2 - World)
    ids = [
        "19TftjXtltd0mWcvrz-dmHKxeekQqJq_U",  # 4.5.1 number of robots installed
        "1EOIw0AlcuRNrEAgILlBo7XvtNHF0yxxo",  # 4.5.2 opearational stock of robots
        "1SSF3AOrmqeWUJyvbZbHNd63Z7k1XON3v",  # 4.5.5 number of robots installed by geographic region (top 5 countries)
        "1D_hIfj97-Pehd4P9NVLqqCguhb_gB4op",  # 4.5.8 professional service robots
    ]

    df_list = []

    try:
        # Mapping for assigning 'Application Area' based on file index
        application_area_map = {0: "Installations", 1: "Operational stock", 2: "Installations"}

        # Possible original column names to standardize the robot count column
        robot_column_names = [
            "Number of Professional Service Robots Installed (in Thousands)",
            "Number of Industrial Robots Installed (in Thousands)",
            "Number of industrial robots (in thousands)",
            "Number of industrial robots installed (in thousands)",
        ]

        for i, drive_id in enumerate(ids):
            df_add = pr.read_csv(common_path + drive_id)

            # Add 'Geographic area' if missing
            if "Geographic area" not in df_add.columns:
                df_add["Geographic area"] = "World"

            # Rename 'Label' to 'Year' if present
            df_add = df_add.rename(columns={"Label": "Year"}) if "Label" in df_add.columns else df_add

            # Add 'Application Area' based on index if not present
            if "Application Area" not in df_add.columns and i in application_area_map:
                df_add["Application Area"] = application_area_map[i]

            # Rename 'Application Area' to 'Indicator'
            df_add = df_add.rename(columns={"Application Area": "Indicator"})

            # Standardize robot count column name
            for col in robot_column_names:
                if col in df_add.columns:
                    df_add = df_add.rename(columns={col: "Number of robots (in thousands)"})

            df_list.append(df_add)
        # Concatenate the DataFrames from the list
        all_dfs = pr.concat(df_list)
        return all_dfs
    except Exception as e:
        raise IOError("Error in fetching or concatenating the data: " + str(e))


if __name__ == "__main__":
    main()

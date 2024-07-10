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
        "1XlRybKjTwZZLGT2i23ZVQyTUwXvHNFA0",  # 4.5.1 number of robots installed
        "1-KiWFJznt_DyYCwR7IV-o7yPoKQk25uQ",  # 4.5.2 opearational stock of robots
        "1Iea7ejTc0EORVgB5Ez6gpfc9ws46itFY",  # 4.5.5 number of robots installed by geographic region (top 5 countries)
        "1gCNYZqP0ctUbY4girVfwdu3_LHvfTS_Z",  # 4.5.8 professional service robots
    ]

    df_list = []

    try:
        # Fetch data from Google Drive and store in a list of DataFrames
        for i, drive_id in enumerate(ids):
            df_add = pr.read_csv(common_path + drive_id)

            if "Geographic area" not in df_add.columns:
                df_add["Geographic area"] = "World"
            if "Year" not in df_add.columns:
                df_add["Year"] = 2022
            if "Application Area" not in df_add.columns:
                if i == 0:
                    df_add["Application Area"] = "Installations"
                elif i == 1:
                    df_add["Application Area"] = "Operational stock"
                elif i == 2:
                    df_add["Application Area"] = "Installations"
            df_add = df_add.rename(columns={"Application Area": "Indicator"})
            if "Number of Professional Service Robots Installed (in Thousands)" in df_add.columns:
                df_add = df_add.rename(
                    columns={
                        "Number of Professional Service Robots Installed (in Thousands)": "Number of robots (in thousands)"
                    }
                )
            elif "Number of industrial robots installed (in thousands)" in df_add.columns:
                df_add = df_add.rename(
                    columns={"Number of industrial robots installed (in thousands)": "Number of robots (in thousands)"}
                )
            elif "Number of industrial robots (in thousands)" in df_add.columns:
                df_add = df_add.rename(
                    columns={"Number of industrial robots (in thousands)": "Number of robots (in thousands)"}
                )
            elif "Number of industrial robots installed (in thousands)" in df_add.columns:
                df_add = df_add.rename(
                    columns={"Number of industrial robots installed (in thousands)": "Number of robots (in thousands)"}
                )

            df_list.append(df_add)

        # Concatenate the DataFrames from the list
        all_dfs = pr.concat(df_list)
        return all_dfs
    except Exception as e:
        raise IOError("Error in fetching or concatenating the data: " + str(e))


if __name__ == "__main__":
    main()

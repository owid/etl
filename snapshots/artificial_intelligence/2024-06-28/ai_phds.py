"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import owid.catalog.processing as pr
from owid.datautils.io import df_to_file

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
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/ai_phds.csv")
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
        "1J-w0NaZyZaVfpnCKUgwkDYhbQqJxm6Ed",  # 6.1.8 graduates by industry
        "1_pLoQhAjneNVWDdoGdMFcUGGwkAAbs7C",  # 4.3.3 share of female graduates in CS
    ]

    df_list = []
    try:
        # Fetch data from Google Drive and store in a list of DataFrames
        for i, drive_id in enumerate(ids):
            df_add = pr.read_csv(common_path + drive_id)

            if "Number of new AI PhD graduates" in df_add.columns and "Sector" in df_add.columns:
                df_add = df_add.rename(columns={"Number of new AI PhD graduates": "value", "Sector": "indicator"})
            elif "New CS PhD graduates (% of total)" and "Gender" in df_add.columns:
                df_add = df_add.rename(columns={"New CS PhD graduates (% of total)": "value", "Gender": "indicator"})
            df_list.append(df_add)

        # Concatenate the DataFrames from the list
        all_dfs = pr.concat(df_list)

        return all_dfs
    except Exception as e:
        raise IOError("Error in fetching or concatenating the data: " + str(e))


if __name__ == "__main__":
    main()

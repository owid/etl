"""Script to create a snapshot of dataset 'Total Conference Attendance (Epoch via AI Index Report, 2023)'."""

from pathlib import Path

import click
import pandas as pd
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
    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/ai_private_investment.csv")
    all_dfs = get_data()
    df_to_file(all_dfs, file_path=snap.path)
    # Add file to DVC and upload to S3.
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
    ids = ["1dRiv5KHWnfp-N3xpC2MfcYkqDPKhF7sQ", "13U0x05ltyt-0tr5OrmOedUR8KBjUbplq"]

    df_list = []

    try:
        # Fetch data from Google Drive and store in a list of DataFrames
        for i, drive_id in enumerate(ids):
            df_add = pd.read_csv(common_path + drive_id)
            # Add "World" to Geographic Area column (usually country in these datasets) if it doesn't exist
            if "Geographic Area" not in df_add.columns:
                df_add["Geographic Area"] = "World"
            df_list.append(df_add)

        # Concatenate the DataFrames from the list
        all_dfs = pd.concat(df_list)

        return all_dfs
    except Exception as e:
        raise IOError("Error in fetching or concatenating the data: " + str(e))


if __name__ == "__main__":
    main()

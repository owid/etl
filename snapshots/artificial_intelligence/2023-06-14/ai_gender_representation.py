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
    all_dfs = get_data()
    # Create a new snapshot.
    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/ai_gender_representation.csv")

    # Download data from source.
    df_to_file(all_dfs, file_path=snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def get_data() -> pd.DataFrame:
    """
    For each google drive ID, fetch all the data from the google drive folder.
    """
    all_dfs = pd.DataFrame()
    common_path = "https://drive.google.com/uc?export=download&id="

    ids = ["1xF33AzZTcFzQFb4HUsHm71wTXtWBMkuX", "1lTtbMj5tNYnktUaBQ96XsZX85TbEYYhM"]
    file_names = ["AI graduates", "CS graduates"]
    for i, id in enumerate(ids):
        df_add = pd.read_csv(common_path + ids[i])
        all_dfs = pd.concat([all_dfs, df_add])
        for col in df_add.columns:
            if col != "Year":
                df_add.rename(columns={col: f"{file_names[i]} - {col}"}, inplace=True)

    return all_dfs


if __name__ == "__main__":
    main()

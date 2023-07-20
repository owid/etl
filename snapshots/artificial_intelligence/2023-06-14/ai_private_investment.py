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


def get_data() -> pd.DataFrame:
    """
    For each google drive ID, fetch all the data from the google drive folder.
    """
    common_path = "https://drive.google.com/uc?export=download&id="
    # IDs of the download files (in google drive) (Investment by focus area 1 - by country and 2 - World)
    ids = ["1dRiv5KHWnfp-N3xpC2MfcYkqDPKhF7sQ", "13U0x05ltyt-0tr5OrmOedUR8KBjUbplq"]

    df_list = []
    for i, id in enumerate(ids):
        df_add = pd.read_csv(common_path + ids[i])
        # Add World to Label column (usually country in these datasets)
        if "Geographic Area" not in df_add.columns:
            df_add["Geographic Area"] = "World"
        df_list.append(df_add)
    all_dfs = pd.concat(df_list)
    return all_dfs


if __name__ == "__main__":
    main()

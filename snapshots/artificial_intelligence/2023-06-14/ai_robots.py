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
    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/ai_robots.csv")

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

    ids = [
        "1MOr_H_ETaSDPGB8-FpTmE-QIYpN_mXjv",
        "1smA1l2PQMbwTVqh6KYIJQ0en0Sdx4JoS",
        "1zoXYo-mE87ClYeC5MWPK1WbSK0Gr_fTk",
        "1cLDadnxoNdc6wqdsZYQJi3a0_nGUNU5H",
        "1RRHZn4caGuk5lE7Rk90Eyc1h14IhCPmI",
        "1ea7IpTuEqMZBga2Ak31AUNs4Q1fDoi8I",
        "1f63iJ_yKA8JEsPCsbhNp5rmlMqFrlTQ6",
    ]
    file_names = [
        "Annual Count",
        "Cumulative Operational",
        "Installed Countries",
        "New Robots Installed",
        "Professional Service Robots",
        "Installed Sectors",
        "Installed Application",
    ]
    for i, id in enumerate(ids):
        df_add = pd.read_csv(common_path + ids[i])
        for col in df_add.columns:
            if col != "Year":
                df_add.rename(columns={col: f"{file_names[i]} - {col}"}, inplace=True)

        all_dfs = pd.concat([all_dfs, df_add])

    return all_dfs


if __name__ == "__main__":
    main()

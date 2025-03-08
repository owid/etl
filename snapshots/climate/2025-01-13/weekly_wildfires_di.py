"""Script to create a snapshot of dataset. It loads data from a specific snapshot on Oct 14, 2024 before the data provider changed the data on wildfires in several countries in Africa."""

from pathlib import Path

import click
import pandas as pd
from owid.datautils.io import df_to_file
from structlog import get_logger

from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()


# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Initialize a new snapshot object for storing data, using a predefined file path structure.
    snap = Snapshot(f"climate/{SNAPSHOT_VERSION}/weekly_wildfires_di.csv")

    # Load old snapshot from Oct 14, 2024 for the DI on comparisons between how the data changed.
    snap_old_data = Snapshot("climate/latest/weekly_wildfires.csv")
    snap_old_data.metadata.outs[0]["md5"] = "57dcb430e9955011bac4bee57b635138"
    snap_old_data.metadata.outs[0]["size"] = 12521342
    snap_old_data.pull()
    df = pd.read_csv(snap_old_data.path)

    # Save the final DataFrame to the specified file path in the snapshot.
    df_to_file(df, file_path=snap.path)  # type: ignore[reportArgumentType]

    # Add the file to DVC and optionally upload it to S3, based on the `upload` parameter.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()

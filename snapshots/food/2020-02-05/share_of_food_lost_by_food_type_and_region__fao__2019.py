from pathlib import Path

import click
import pandas as pd

from etl.backport_helpers import long_to_wide
from etl.snapshot import Snapshot

SNAPSHOT_NAMESPACE = Path(__file__).parent.parent.name
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Load backported snapshot.
    snap_values = Snapshot(
        "backport/latest/dataset_4913_share_of_food_lost_by_food_type_and_region__fao__2019_values.feather"
    )
    snap_values.pull()
    # Create a new snapshot. Metadata is hardcoded in the accompanying DVC file.
    snap = Snapshot("food/2020-02-05/share_of_food_lost_by_food_type_and_region__fao__2019.feather")

    # Convert from long to wide format.
    df = long_to_wide(pd.read_feather(snap_values.path))

    # Copy file to the new snapshot.
    snap.path.parent.mkdir(parents=True, exist_ok=True)
    df.reset_index().to_feather(snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()

from pathlib import Path

import click
import pandas as pd

from etl.backport_helpers import long_to_wide
from etl.snapshot import Snapshot, SnapshotMeta

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
        "backport/latest/dataset_3009_share_of_agriculture_in_gdp_at_current_prices__herrendorf_et_al__and_ggdc_10_data__split_values.feather"
    )
    snap_values.pull()
    snap_config = Snapshot(
        "backport/latest/dataset_3009_share_of_agriculture_in_gdp_at_current_prices__herrendorf_et_al__and_ggdc_10_data__split_config.json"
    )
    snap_config.pull()

    # Create snapshot metadata for the new file
    snap = Snapshot(f"{SNAPSHOT_NAMESPACE}/{SNAPSHOT_VERSION}/share_of_agriculture_in_gdp.feather")

    meta = SnapshotMeta(**snap_values.metadata.to_dict())
    meta.namespace = SNAPSHOT_NAMESPACE
    meta.version = SNAPSHOT_VERSION
    meta.short_name = "share_of_agriculture_in_gdp"
    meta.is_public = True
    meta.fill_from_backport_snapshot(snap_config.path)

    snap.metadata = meta

    # Convert from long to wide format.
    df = long_to_wide(pd.read_feather(snap_values.path))

    # Copy file to the new snapshot.
    snap.path.parent.mkdir(parents=True, exist_ok=True)
    df.reset_index().to_feather(snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()

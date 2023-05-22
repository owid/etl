import shutil
from pathlib import Path

import click

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
    # Load backported snapshot with values.
    snap_values = Snapshot(
        "backport/latest/dataset_5429_excess_mortality_estimates__the_economist__2023_values.feather"
    )
    snap_values.pull()

    # Excess mortality dataset is private
    snap_values.metadata.is_public = False

    # Create snapshot metadata for the new file
    meta = SnapshotMeta(**snap_values.metadata.to_dict())
    meta.namespace = SNAPSHOT_NAMESPACE
    meta.version = SNAPSHOT_VERSION
    meta.short_name = "excess_mortality_economist"
    meta.save()

    # Create a new snapshot.
    snap = Snapshot(meta.uri)

    # Copy file to the new snapshot.
    snap.path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(snap_values.path, snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()

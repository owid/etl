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
        "backport/latest/dataset_2482_gini_coefficient__equivalized_income_after_tax_and_transfers__chartbook_of_economic_inequality__2017_values.feather"
    )
    snap_values.pull()
    # Create a new snapshot. Metadata is hardcoded in the accompanying DVC file.
    snap = Snapshot(
        "poverty_inequality/2018-02-27/gini_coefficient__equivalized_income_after_tax_and_transfers__chartbook_of_economic_inequality__2017.feather"
    )

    # Convert from long to wide format.
    df = long_to_wide(pd.read_feather(snap_values.path))

    # Copy file to the new snapshot.
    snap.path.parent.mkdir(parents=True, exist_ok=True)
    df.reset_index().to_feather(snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()

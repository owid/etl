"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    # Load data from Oct 14, 2024 snapshot.
    snap = Snapshot("climate/latest/weekly_wildfires.csv")
    snap.metadata.outs[0]["md5"] = "57dcb430e9955011bac4bee57b635138"  # Oct 14, 2024
    snap.metadata.outs[0]["size"] = 12521342
    snap.pull()
    tb = snap.read()
    #
    # Process data.
    #

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "month_day", "year", "indicator"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

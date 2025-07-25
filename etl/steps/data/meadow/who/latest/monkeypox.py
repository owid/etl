"""Load a snapshot and create a meadow dataset."""

import structlog

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

log = structlog.get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("monkeypox.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    # Remove rows with null values
    tb = tb.dropna(subset=["DATE", "ISO3"], how="all")

    # Check for and remove NaT values in DATE column
    initial_rows = len(tb)
    tb = tb.dropna(subset=["DATE"])
    final_rows = len(tb)

    if initial_rows > final_rows:
        log.warning(f"Dropped {initial_rows - final_rows} rows with NaT values in DATE column")

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

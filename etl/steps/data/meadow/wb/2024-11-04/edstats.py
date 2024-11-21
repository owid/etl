"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("edstats.csv")

    # Load data from snapshot.
    tb = snap.read(low_memory=False)

    #
    # Process data.
    #
    # Rename indicator code and name columns
    tb = tb.rename(columns={"Series": "indicator_name", "wb_seriescode": "indicator_code"})

    tb = tb.format(["country", "year", "indicator_name"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

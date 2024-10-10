"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("gram_level.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    tb = tb.rename(columns={"Location": "country", "Year": "year"}, errors="raise")
    # Some unexpected duplicates with different values - we'll drop both cases
    tb = tb.drop_duplicates(subset=["country", "year", "ATC level 3 class"])
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "atc_level_3_class"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("mortality_database.feather")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = tb.drop(columns=["Region Code", "Country Code", "Age group code", "Region Name"])
    tb = tb.rename(columns={"Country Name": "country"}, errors="raise")
    tb = tb.format(["country", "year", "sex", "age_group", "cause"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

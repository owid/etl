"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("glass_enrolment.xlsx")

    # Load data from snapshot.
    tb = snap.read()
    # Drop the rows where there isn't a country name
    tb = tb.dropna(subset=["Code"])

    # Check the number of countries
    assert len(tb["Country"] == 197)
    # Rename columns
    tb = tb.drop(columns=["Country"]).rename(columns={"Label": "country"})
    tb["year"] = snap.metadata.origin.date_published.split("-")[0]
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

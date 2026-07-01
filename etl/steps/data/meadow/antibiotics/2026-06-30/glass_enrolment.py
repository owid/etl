"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

YEAR_GIVEN = 2024  # check on website: https://worldhealthorg.shinyapps.io/glass-dashboard/_w_40cdd046f5ef42ad90bcd40e74452236/#!/home


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("glass_enrolment.xlsx")

    # Load data from snapshot.
    tb = snap.read()
    # Drop the rows where there isn't a country name
    tb = tb.dropna(subset=["Code"])

    # Check the number of countries (196)
    # Hong Kong got removed in 2024 update
    assert len(tb["Country"]) == 196, f"{len(tb['Country'])} countries found, expected 196"

    # Rename columns
    tb = tb.drop(columns=["Country"]).rename(columns={"Label": "country"})
    tb["year"] = YEAR_GIVEN

    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

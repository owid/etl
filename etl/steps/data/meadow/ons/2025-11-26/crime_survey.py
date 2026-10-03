"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("crime_survey.csv")

    # Load data from snapshot.
    tb = snap.read(skiprows=6)
    tb = tb.rename(columns={"Unnamed: 0": "year"})
    # Split date column into month and year
    tb["month"] = tb["year"].str[:3]
    tb["year"] = tb["year"].str[-4:]
    tb["country"] = "England and Wales"

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

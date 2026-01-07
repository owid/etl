"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("bmi.csv")

    # Load data from snapshot.
    tb = snap.read()
    tb = tb[["Body Mass Index", "Geopolitical entity (reporting)", "TIME_PERIOD", "OBS_VALUE"]]
    tb = tb.rename(columns={"Geopolitical entity (reporting)": "country", "TIME_PERIOD": "year"})

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["country", "year", "body_mass_index"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

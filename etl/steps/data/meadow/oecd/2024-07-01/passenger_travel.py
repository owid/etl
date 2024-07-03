"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLS_TO_KEEP = ["Reference area", "Vehicle type", "TIME_PERIOD", "OBS_VALUE"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("passenger_travel.csv")

    # Load data from snapshot.
    tb = snap.read()

    tb = tb[COLS_TO_KEEP]

    # Rename columns
    tb = tb.rename(columns={"Reference area": "country", "TIME_PERIOD": "year"})

    # recast year and obs value column to int
    tb["year"] = tb["year"].astype("Int64")
    tb["OBS_VALUE"] = tb["OBS_VALUE"].astype("Int64")

    # drop rows where year is null
    tb = tb.dropna(subset=["year"])

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "vehicle_type"])
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

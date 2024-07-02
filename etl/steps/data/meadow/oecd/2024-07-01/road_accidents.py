"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLS_TO_DROP = [
    "STRUCTURE",
    "STRUCTURE_ID",
    "STRUCTURE_NAME",
    "ACTION",
    "REF_AREA",
    "FREQ",
    "Frequency of observation",
    "MEASURE",
    "UNIT_MEASURE",
    "TRANSPORT_MODE",
    "Transport mode",
    "VEHICLE_TYPE",
    "Vehicle type",
    "INFRASTRUCTURE_TYPE",
    "Infrastructure type",
    "Time period",
    "Observation value",
    "OBS_STATUS",
    "Observation status",
    "UNIT_MULT",
    "Unit multiplier",
    "DECIMALS",
    "Decimals",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("road_accidents.csv")

    # Load data from snapshot.
    tb = snap.read()

    # drop unneeded columns
    tb = tb.drop(columns=COLS_TO_DROP)

    # Rename columns
    tb = tb.rename(columns={"Reference area": "country", "TIME_PERIOD": "year"})

    # recast year and obs value column to int
    tb["year"] = tb["year"].astype("Int64")
    tb["OBS_VALUE"] = tb["OBS_VALUE"].astype("Int64")

    # drop rows where year is null
    tb = tb.dropna(subset=["year"])

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "measure"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

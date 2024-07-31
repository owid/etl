"""Load a snapshot and create a meadow dataset."""
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

RENAME_COLUMNS = {
    "REF_AREA:Geographic area": "country",
    "INDICATOR:Indicator": "indicator",
    "AGE:Current age": "age",
    "TIME_PERIOD:Time period": "year",
    "OBS_VALUE:Observation Value": "value",
    "STAT_POP:Statistical Population": "stat_pop",
    "UNIT_MEASURE:Unit of measure": "unit",
    "UNIT_MULTIPLIER:Unit multiplier": "unit_multiplier",
}

COLUMNS_TO_KEEP = [
    "REF_AREA:Geographic area",
    "INDICATOR:Indicator",
    "AGE:Current age",
    "TIME_PERIOD:Time period",
    "OBS_VALUE:Observation Value",
    "STAT_POP:Statistical Population",
    "UNIT_MEASURE:Unit of measure",
    "UNIT_MULTIPLIER:Unit multiplier",
]

# NA is one because it should not affect the value when multiplying
UNIT_MAP = {"3: Thousands": 1000, "0 :Units": 1, "NA": 1}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("child_migration.csv")

    # Load data from snapshot.
    tb = snap.read()

    # Rename columns.
    tb = tb[COLUMNS_TO_KEEP]
    tb = tb.rename(columns=RENAME_COLUMNS, errors="raise")

    # multiply by unit multiplier
    tb["unit_multiplier"] = tb["unit_multiplier"].fillna("NA").map(UNIT_MAP)
    tb["value"] = tb["value"].replace("<1", "0").astype("Float64") * tb["unit_multiplier"]

    # filter only on age 0-17
    tb = tb[tb["age"] == "Y0T17: Under 18 years old"]

    # drop unneeded columns
    tb = tb.drop(columns=["unit_multiplier", "age"])

    # drop duplicated rows
    tb = tb.drop_duplicates()

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "indicator", "stat_pop"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLUMN_MAPPING = {
    "Country name": "country",
    "year": "year",
    "Region indicator": "region",
    "Age group code": "age_group_code",
    "Age group": "age_group",
    "Mean of ladder": "happiness_score",
    "Mean of stress": "stress_score",
    "Mean of worry": "worry_score",
    "Count of ladder": "happiness_count",
    "Count of stress": "stress_count",
    "Count of worry": "worry_count",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("happiness_ages.xls")

    # Load data from snapshot.
    tb = snap.read()

    # rename columns
    tb = tb.rename(columns=COLUMN_MAPPING, errors="raise")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "age_group"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

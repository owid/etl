"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to keep, and their new names
COLUMNS_TO_KEEP = {"Year": "year", "Total Population": "Both sexes", "Males": "Males", "Females": "Females"}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("population_ireland.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Assert expected columns are in the table
    expected_columns = set(COLUMNS_TO_KEEP.keys())
    actual_columns = set(tb.columns)
    missing_columns = expected_columns - actual_columns
    assert not missing_columns, f"The following expected columns are missing from the input data: {missing_columns}"

    # Keep and rename relevant columns
    tb = tb.rename(columns=COLUMNS_TO_KEEP, errors="raise")[list(COLUMNS_TO_KEEP.values())]

    # Add country column
    tb["country"] = "Ireland"

    # Make table long
    tb = tb.melt(id_vars=["country", "year"], var_name="sex", value_name="population")

    # Improve tables format.
    tables = [tb.format(["country", "sex", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

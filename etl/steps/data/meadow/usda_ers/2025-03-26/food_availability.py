"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from the data, and how to rename them.
COLUMNS = {
    "Year": "year",
    "Food energy": "daily_calories",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots and read their data.
    snap = paths.load_snapshot("food_availability.xls")
    data = snap.read(safe_types=False, sheet_name="Totals", skiprows=1)

    #
    # Process data.
    #
    # Select and rename columns.
    tb = data[COLUMNS.keys()].rename(columns=COLUMNS)

    # Drop any row for which "year" is not an integer (to get rid of headers and footers).
    tb = tb[tb["year"].apply(lambda x: isinstance(x, int))].reset_index(drop=True)

    # Add a country column.
    tb["country"] = "United States"

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])

    # Save meadow dataset.
    ds_meadow.save()

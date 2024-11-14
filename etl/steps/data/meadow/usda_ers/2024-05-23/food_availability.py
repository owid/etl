"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from the data, and how to rename them.
COLUMNS = {
    "Year": "year",
    "Food energy": "daily_calories",
}


def run(dest_dir: str) -> None:
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

    # Format table conveniently.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()

"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Select and rename columns.
COLUMNS = {
    "year": "year",
    "global_avg__cell_price__dollar_kwh": "battery_cell_price",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("battery_cell_prices")

    # Read table from meadow dataset.
    tb = ds_meadow.read("battery_cell_prices")

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[COLUMNS.keys()].rename(columns=COLUMNS, errors="raise")

    # Clean year column.
    tb["year"] = tb["year"].str.strip().str[0:4].astype("Int64")

    # Add country column.
    tb["country"] = "World"

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()

"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("mass_balance_us_glaciers")
    tb = ds_meadow.read("mass_balance_us_glaciers")

    #
    # Process data.
    #
    # Change column names to human-readable names.
    tb = tb.rename(
        columns={column: column.replace("_", " ").title() for column in tb.columns if column != "year"}, errors="raise"
    )

    # Transpose table to have location as a column.
    tb = tb.melt(id_vars=["year"], var_name="location", value_name="mass_balance_us_glaciers")

    # Remove empty rows.
    tb = tb.dropna().reset_index(drop=True)

    # Set an appropriate index to each table and sort conveniently.
    tb = tb.format(["location", "year"], sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()

from etl.helpers import PathFinder

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)

# Columns to use, and how to rename them (they will become entities).
COLUMNS = {
    "year": "year",
    "li_ion_batteries_all_cells_price_global__representative__usd__2024__kwh": "price",
    "li_ion_batteries_all_li_ion_batteries_cumulative_production_global__representative__gwh": "cumulative_production",
}


def run() -> None:
    #
    # Load data.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("historical_battery_costs")
    tb = ds_meadow.read("historical_battery_costs")

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Remove empty rows.
    tb = tb.dropna().reset_index(drop=True)

    # Improve tables format.
    tb = tb.format(["year"])

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as meadow
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()

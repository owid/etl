from etl.helpers import PathFinder

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)

# Columns to use, and how to rename them.
COLUMNS = {
    "year": "year",
    "crude_oil_price__2024dollar_mwh__useful": "crude_oil_price",
    "coal_price__average_usa__2024dollar_mwh__useful": "coal_price",
    "gas_price__us__2024dollar_mwh__useful": "gas_price",
    "coal_lcoe__us_global__2024dollar_mwh__useful": "coal_electricity_lcoe",
    "gas_lcoe__us_global__2024dollar_mwh__useful": "gas_electricity_lcoe",
    "nuclear_lcoe__us__2024dollar_mwh__useful": "nuclear_electricity_lcoe",
    "wind_onshore_lcoe__global__2024dollar_mwh__useful": "wind_onshore_electricity_lcoe",
    "solar_pv_lcoe__global__2024dollar_mwh__useful": "solar_pv_electricity_lcoe",
}


def run() -> None:
    #
    # Load data.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("historical_energy_costs")
    tb = ds_meadow.read("historical_energy_costs")

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Improve tables format.
    tb = tb.format(["year"])

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as meadow
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()

from etl.helpers import PathFinder

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)

# Columns to use, and how to rename them (they will become entities).
COLUMNS = {
    "year": "year",
    "crude_oil_price__2024dollar_mwh__useful": "Crude oil",
    "coal_price__average_usa__2024dollar_mwh__useful": "Coal",
    "gas_price__us__2024dollar_mwh__useful": "Gas",
    "coal_lcoe__us_global__2024dollar_mwh__useful": "Coal electricity",
    "gas_lcoe__us_global__2024dollar_mwh__useful": "Gas electricity",
    "nuclear_lcoe__us__2024dollar_mwh__useful": "Nuclear electricity",
    "wind_onshore_lcoe__global__2024dollar_mwh__useful": "Wind onshore electricity",
    "solar_pv_lcoe__global__2024dollar_mwh__useful": "Solar PV electricity",
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

    # Transpose table.
    tb = tb.melt(id_vars=["year"], value_name="price", var_name="technology")

    # Drop empty rows.
    tb = tb.dropna(subset="price").reset_index(drop=True)

    # Improve tables format.
    tb = tb.format(["technology", "year"])

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as meadow
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()

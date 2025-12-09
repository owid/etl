from etl.helpers import PathFinder

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)

# Columns to use, and how to rename them (they will become entities).
COLUMNS = {
    "year": "year",
    # "Li-ion batteries All cells Price Global (Representative), USD(2024)/kWh",
    "li_ion_batteries_all_cells_price_global__representative__usd__2024__kwh": "price",
    # "Li-ion batteries All Li-ion batteries Cumulative production Global (Representative), GWh",
    "li_ion_batteries_all_li_ion_batteries_cumulative_production_global__representative__gwh": "cumulative_production",
    # Here are some additional columns we could use (copied from the meadow step).
    # For now, I'll ignore them:
    # # Price.
    # # "Li-ion batteries Cylindrical cells Price Global (Representative), USD(2024)/kWh",
    # 'li_ion_batteries_cylindrical_cells_price_global__representative__usd__2024__kwh',
    # # "Li-ion batteries EV battery pack Price Global (Representative), USD(2024)/kWh",
    # 'li_ion_batteries_ev_battery_pack_price_global__representative__usd__2024__kwh',
    # # "Li-ion batteries Utility-scale BESS Cost Global (Representative), USD(2024)/kWh",
    # 'li_ion_batteries_utility_scale_bess_cost_global__representative__usd__2024__kwh',
    # # "Li-ion batteries Residential BESS Cost Germany (Representative), USD(2024)/kWh",
    # 'li_ion_batteries_residential_bess_cost_germany__representative__usd__2024__kwh',
    # # Annual production and annual additions.
    # # "Li-ion batteries All Li-ion batteries Annual production Global (Representative), GWh/yr",
    # 'li_ion_batteries_all_li_ion_batteries_annual_production_global__representative__gwh_yr',
    # # "Li-ion batteries EV batteries Annual additions Global (Representative), GWh/yr",
    # 'li_ion_batteries_ev_batteries_annual_additions_global__representative__gwh_yr',
    # # "Li-ion batteries Utility-scale BESS Annual additions Global (Representative), GWh/yr",
    # 'li_ion_batteries_utility_scale_bess_annual_additions_global__representative__gwh_yr',
    # # Cumulative production.
    # # NOTE: In Rupert's data the EV and BESS cumulative series are in GWh/yr, but they are cumulative stocks, so I understand they should be in GWh.
    # # "Li-ion batteries EV batteries Cumulative additions Global (Representative), GWh/yr",
    # 'li_ion_batteries_ev_batteries_cumulative_additions_global__representative__gwh_yr',
    # # "Li-ion batteries Utility-scale BESS Cumulative additions Global (Representative), GWh/yr",
    # 'li_ion_batteries_utility_scale_bess_cumulative_additions_global__representative__gwh_yr'],
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

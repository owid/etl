"""Compilation of energy prices datasets."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load data.
    #
    # Load Eurostat data on European gas and electricity prices.
    ds_eurostat = paths.load_dataset("gas_and_electricity_prices")
    tb_eurostat_euro = ds_eurostat.read("gas_and_electricity_price_components_euro_flat")
    tb_eurostat_pps = ds_eurostat.read("gas_and_electricity_price_components_pps_flat")

    # Load Ember data on European wholesale electricity prices.
    ds_ember = paths.load_dataset("european_wholesale_electricity_prices")
    tb_ember_monthly = ds_ember.read("european_wholesale_electricity_prices_monthly")
    tb_ember_annual = ds_ember.read("european_wholesale_electricity_prices_annual")

    #
    # Process data.
    #
    # Rename columns in all tables to have consistent dimensions.
    tb_eurostat_euro = tb_eurostat_euro.rename(
        columns={
            column: f"annual_{column}" for column in tb_eurostat_euro.columns if column not in ["country", "year"]
        },
        errors="raise",
    )
    tb_eurostat_pps = tb_eurostat_pps.rename(
        columns={column: f"annual_{column}" for column in tb_eurostat_pps.columns if column not in ["country", "year"]},
        errors="raise",
    )
    tb_ember_monthly = tb_ember_monthly.rename(
        columns={"price": "monthly_electricity_all_wholesale_euro"}, errors="raise"
    )
    tb_ember_annual = tb_ember_annual.rename(columns={"price": "annual_electricity_all_wholesale_euro"}, errors="raise")

    # Add dimensions
    tb_ember_monthly.monthly_electricity_all_wholesale_euro.m.dimensions = {
        "consumer_type": "All",
        "price_component_or_level": "Wholesale",
        "source": "Electricity",
    }
    tb_ember_annual.annual_electricity_all_wholesale_euro.m.dimensions = {
        "consumer_type": "All",
        "price_component_or_level": "Wholesale",
        "source": "Electricity",
    }

    # Check that all columns in all tables have dimensions
    for table in [tb_eurostat_euro, tb_eurostat_pps, tb_ember_annual]:
        for column in table.columns:
            if column not in ["country", "year"]:
                assert (
                    table[column].metadata.dimensions is not None
                ), f"Column {column} in table {table.m.short_name} has no dimensions."

    # Create a combined annual table.
    tb_annual = pr.multi_merge(
        tables=[tb_eurostat_euro, tb_eurostat_pps, tb_ember_annual], on=["country", "year"], how="outer"
    )
    ####################################################################################################################
    # Add combined description processing (since propagation does not support this):
    description_processing_pps = tb_eurostat_pps[
        "annual_electricity_household_total_price_including_taxes_pps"
    ].metadata.description_processing
    description_processing_euros = tb_eurostat_euro[
        "annual_electricity_household_total_price_including_taxes_euro"
    ].metadata.description_processing
    for column in tb_annual.columns:
        if "pps" in column:
            assert tb_annual[column].metadata.description_processing is None
            tb_annual[column].metadata.description_processing = description_processing_pps

            # Add dimension
            tb_annual[column].m.dimensions["unit"] = "pps"

        elif "euro" in column:
            assert tb_annual[column].metadata.description_processing is None
            tb_annual[column].metadata.description_processing = description_processing_euros

            # Add dimension
            tb_annual[column].m.dimensions["unit"] = "euro"

    # Add combined description processing for wholesale prices.
    tb_annual["annual_electricity_all_wholesale_euro"].metadata.description_processing = tb_ember_annual[
        "annual_electricity_all_wholesale_euro"
    ].metadata.description_processing
    ####################################################################################################################
    tb_annual = tb_annual.format(short_name="energy_prices_annual")

    # Add frequency dimensions
    tb_ember_monthly["monthly_electricity_all_wholesale_euro"].m.dimensions["frequency"] = "monthly"

    for col in tb_annual.columns:
        tb_annual[col].m.dimensions["frequency"] = "annual"

    # Create a combined monthly table.
    # For now, only Ember has monthly data.
    tb_monthly = tb_ember_monthly.copy()
    tb_monthly = tb_monthly.format(keys=["country", "date"], short_name="energy_prices_monthly")

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as meadow
    ds_garden = paths.create_dataset(tables=[tb_annual, tb_monthly])
    ds_garden.save()

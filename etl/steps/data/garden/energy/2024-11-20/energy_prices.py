"""Compilation of energy prices datasets.

"""
import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load Eurostat data on European gas and electricity prices.
    ds_eurostat = paths.load_dataset("gas_and_electricity_prices")
    tb_eurostat_euro = ds_eurostat.read("gas_and_electricity_price_components_euro_flat")
    tb_eurostat_pps = ds_eurostat.read("gas_and_electricity_price_components_pps_flat")

    # Load Ember data on European wholesale electricity prices.
    ds_ember = paths.load_dataset("european_wholesale_electricity_prices")
    tb_ember = ds_ember.read("european_wholesale_electricity_prices")

    #
    # Process data.
    #
    # Prepare eurostat columns.
    # TODO: Instead of this, consider having just one table for both euro and pps.
    tb_eurostat_euro = tb_eurostat_euro.rename(
        columns={column: f"{column}_euro" for column in tb_eurostat_euro.columns if column not in ["country", "year"]},
        errors="raise",
    )

    # Rename columns to match the Eurostat table.
    # TODO: Ensure euros are consistent (possibly convert both to constant USD, for consistency with battery prices?).
    tb_ember = tb_ember.rename(columns={"price": "electricity_all_wholesale_euro"}, errors="raise")

    # Ember provides monthly data, so we can create a monthly table of wholesale electricity prices.
    # But we also need to create an annual table.
    tb_ember_annual = tb_ember.copy()
    tb_ember_annual["year"] = tb_ember_annual["date"].str[:4].astype("Int64")

    # Create a table of annual average wholesale electricity prices.
    # TODO: Consider removing incomplete years.
    tb_ember_annual = tb_ember_annual.groupby(["country", "year"], observed=True, as_index=False).agg(
        {"electricity_all_wholesale_euro": "mean"}
    )

    # Create a combined annual table.
    tb_annual = pr.multi_merge(
        tables=[tb_eurostat_euro, tb_eurostat_pps, tb_ember_annual], on=["country", "year"], how="outer"
    )
    tb_annual = tb_annual.format(short_name="energy_prices_annual")

    # Create a combined monthly table.
    # For now, only Ember has monthly data.
    tb_monthly = tb_ember.copy()
    tb_monthly = tb_monthly.format(keys=["country", "date"], short_name="energy_prices_monthly")

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as meadow
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb_annual, tb_monthly], check_variables_metadata=True)
    ds_garden.save()

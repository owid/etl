"""Garden step for WSTS Historical Billings Report dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("wsts_historical_billings")

    # Read tables from meadow dataset.
    tb_monthly = ds_meadow["wsts_historical_billings_monthly"].reset_index()
    tb_3mma = ds_meadow["wsts_historical_billings_3mma"].reset_index()

    # Load US CPI data for inflation adjustment
    ds_us_cpi = paths.load_dataset("us_consumer_prices")
    tb_us_cpi = ds_us_cpi.read("us_consumer_prices")

    #
    # Process data.
    #
    # Harmonize country names
    tb_monthly = geo.harmonize_countries(df=tb_monthly, countries_file=paths.country_mapping_path, country_col="region")
    tb_3mma = geo.harmonize_countries(df=tb_3mma, countries_file=paths.country_mapping_path, country_col="region")

    # Process monthly/quarterly data with 3 months running averages
    # Convert month name to month number
    tb_3mma["date"] = pd.to_datetime(tb_3mma["year"].astype(str) + "-" + tb_3mma["month"].astype(str), format="%Y-%B")

    # Drop the separate year and month columns since we now have date
    tb_3mma = tb_3mma.drop(columns=["year", "month"])

    # Extract yearly data
    tb_yearly = tb_monthly[tb_monthly["period"] == "Total Year"].copy()
    tb_yearly = tb_yearly.drop(columns=["period", "period_type"])

    # Adjust for inflation using the US Consumer Price Index (CPI)
    tb_yearly = add_inflation_adjusted_values(tb_yearly, tb_us_cpi)

    # Format tables
    tb_yearly = tb_yearly.format(["region", "year"], short_name="wsts_historical_billings_yearly")
    tb_3mma = tb_3mma.format(["region", "date"], short_name="wsts_historical_billings_3mma")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb_yearly, tb_3mma], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_inflation_adjusted_values(tb_yearly: Table, tb_us_cpi: Table) -> Table:
    """
    Add inflation-adjusted billing values using US CPI with 2021 as base year.

    Args:
        tb_yearly: Table with yearly billing data containing 'value' column
        tb_us_cpi: Table with US CPI data containing 'all_items' and 'year' columns

    Returns:
        Table with additional 'value_constant_2021_usd' column
    """
    # Calculate CPI adjustment factor with 2021 as base year
    cpi_2021 = tb_us_cpi.loc[tb_us_cpi["year"] == 2021, "all_items"].values[0]
    tb_us_cpi["cpi_adj_2021"] = tb_us_cpi["all_items"] / cpi_2021
    tb_us_cpi_2021 = tb_us_cpi[["cpi_adj_2021", "year"]].copy()

    # Merge CPI data with yearly billings
    tb_yearly = pr.merge(tb_yearly, tb_us_cpi_2021, on="year", how="inner")

    # Create inflation-adjusted value column
    tb_yearly["value_constant_2021_usd"] = round(tb_yearly["value"] / tb_yearly["cpi_adj_2021"])

    # Drop the temporary CPI adjustment column
    tb_yearly = tb_yearly.drop("cpi_adj_2021", axis=1)

    return tb_yearly

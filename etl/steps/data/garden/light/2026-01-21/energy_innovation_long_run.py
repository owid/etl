"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define lumens suitable fore reading
LUMENS_FOR_READING = 800


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("energy_innovation_long_run")

    # Read table from meadow dataset.
    tb_lighting_prices = ds_meadow.read("lighting_prices")
    tb_energy_consumption = ds_meadow.read("energy_consumption")

    # Load earnings from A milllennium of macroeconomic data
    ds_earnings = paths.load_dataset("millennium_macroeconomic_data")
    tb_earnings = ds_earnings.read("millennium_macroeconomic_data")

    #
    # Process data.
    #

    # Process lighting prices table to make it long with dimensions.
    tb_lighting_prices = process_lighting_prices(tb_lighting_prices=tb_lighting_prices)

    # Add rolling average to lighting prices
    tb_lighting_prices = add_rolling_average(tb=tb_lighting_prices)

    # Calculate weeks of earnings needed for reading
    tb_weeks_of_earnings = calculate_weeks_of_earnings_needed_for_reading(
        tb_lighting_prices=tb_lighting_prices,
        tb_earnings=tb_earnings,
    )

    # Process energy consumption table to make it long with dimensions.
    tb_energy_consumption = process_energy_consumption(tb=tb_energy_consumption)

    # Improve table format.
    tb_lighting_prices = tb_lighting_prices.format(["country", "year", "lighting_source", "price_year"])
    tb_weeks_of_earnings = tb_weeks_of_earnings.format(
        ["country", "year"], short_name="weeks_of_earnings_needed_for_reading"
    )
    tb_energy_consumption = tb_energy_consumption.format(["country", "year", "lighting_source"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[tb_lighting_prices, tb_weeks_of_earnings, tb_energy_consumption], default_metadata=ds_meadow.metadata
    )

    # Save garden dataset.
    ds_garden.save()


def process_lighting_prices(tb_lighting_prices: Table) -> Table:
    """Process lighting prices table, making it long with dimensions."""

    # Make the table long
    tb_lighting_prices = tb_lighting_prices.melt(
        id_vars=["country", "year"],
        var_name="measure",
        value_name="value",
    )

    # Extract lighting_source and price_year from measure column
    tb_lighting_prices["lighting_source"] = tb_lighting_prices["measure"].str.extract(
        r"lighting_prices_(.+?)_\d{4}prices", expand=False
    )
    tb_lighting_prices["price_year"] = (
        tb_lighting_prices["measure"].str.extract(r"lighting_prices_.+?_(\d{4})prices", expand=False).astype(int)
    )

    # Drop the original measure column
    tb_lighting_prices = tb_lighting_prices.drop(columns=["measure"])

    # Rename value column
    tb_lighting_prices = tb_lighting_prices.rename(columns={"value": "lighting_price"}, errors="raise")

    # Replace underscore with space in lighting_source
    tb_lighting_prices["lighting_source"] = tb_lighting_prices["lighting_source"].str.replace("_", " ")

    # For lighting_price, make all zero values NaN
    tb_lighting_prices["lighting_price"] = tb_lighting_prices["lighting_price"].replace(0, pd.NA)

    return tb_lighting_prices


def calculate_weeks_of_earnings_needed_for_reading(
    tb_lighting_prices: Table,
    tb_earnings: Table,
) -> Table:
    """
    Calculate the number of weeks needed to afford lighting for reading based on earnings.
    """

    tb_lighting_prices = tb_lighting_prices.copy()
    tb_earnings = tb_earnings.copy()

    # In tb_earnings, create cpi_base, with the value of cpi for the year 2000 (for each country)

    tb_earnings["cpi_base_year"] = tb_earnings.groupby("country")["cpi"].transform(
        lambda x: x.loc[tb_earnings["year"] == 2000].values[0]
    )

    # Calculate real_average_weekly_earnings as average_weekly_earnings * cpi_base/cpi
    tb_earnings["real_average_weekly_earnings"] = tb_earnings["average_weekly_earnings"] * (
        tb_earnings["cpi_base_year"] / tb_earnings["cpi"]
    )

    # For lighting prices, select lighting source "average" and price_year 2000
    tb_lighting_prices_avg_2000 = tb_lighting_prices[
        (tb_lighting_prices["lighting_source"] == "average") & (tb_lighting_prices["price_year"] == 2000)
    ][["country", "year", "lighting_price"]]

    # Re-estimate lighting price for LUMENS_FOR_READING lumens
    tb_lighting_prices_avg_2000["lighting_price_reading"] = (
        tb_lighting_prices_avg_2000["lighting_price"] * LUMENS_FOR_READING / 1e6
    )

    # Merge both tables on country and year
    tb = pr.merge(tb_lighting_prices_avg_2000, tb_earnings, on=["country", "year"], how="inner")

    # Calculate weeks_of_earnings_needed_for_lighting
    tb["weeks_of_earnings_needed_for_reading"] = tb["lighting_price_reading"] / tb["real_average_weekly_earnings"]

    # Convert weeks to days
    tb["days_of_earnings_needed_for_reading"] = tb["weeks_of_earnings_needed_for_reading"] * 5

    # Keep only relevant columns
    tb = tb[["country", "year", "weeks_of_earnings_needed_for_reading", "days_of_earnings_needed_for_reading"]]

    return tb


def add_rolling_average(tb: Table) -> Table:
    """
    Add a 5-year rolling average to the table.
    """
    tb = tb.sort_values(by=["country", "year", "lighting_source", "price_year"])
    tb["lighting_price_rolling_avg"] = tb.groupby(["country", "lighting_source", "price_year"])[
        "lighting_price"
    ].transform(lambda x: x.rolling(window=5, min_periods=1).mean())
    return tb


def process_energy_consumption(tb: Table) -> Table:
    """Process energy consumption table, making it long with dimensions."""

    # Make the table long
    tb = tb.melt(
        id_vars=["country", "year"],
        var_name="lighting_source",
        value_name="share_energy_consumption",
    )

    # Extract lighting_source and price_year from measure column
    tb["lighting_source"] = tb["lighting_source"].str.extract(r"energy_consumption_(.+)", expand=False)

    # Replace underscore with space in lighting_source
    tb["lighting_source"] = tb["lighting_source"].str.replace("_", " ")

    # Multply share_energy_consumption by 100 to convert to percentage
    tb["share_energy_consumption"] *= 100

    return tb

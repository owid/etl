"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define lumens suitable fore reading
LUMENS_FOR_READING = 800

# Set if we expand earnings series or not
EXPAND_EARNING_SERIES = True

# Set debug mode
DEBUG = False


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("energy_innovation_long_run")

    # Read table from meadow dataset.
    tb_lighting_prices = ds_meadow.read("lighting_prices")

    # Load earnings from A milllennium of macroeconomic data
    ds_earnings = paths.load_dataset("millennium_macroeconomic_data")
    tb_earnings = ds_earnings.read("millennium_macroeconomic_data")

    # Load CPI UK
    ds_cpi = paths.load_dataset("cpi_uk")
    tb_cpi = ds_cpi.read("cpi_uk")

    # Load modern UK earnings series
    ds_earnings_new = paths.load_dataset("average_weekly_earnings_uk")
    tb_earnings_new = ds_earnings_new.read("average_weekly_earnings_uk")

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
        tb_earnings_new=tb_earnings_new,
        tb_cpi=tb_cpi,
    )

    # Improve table format.
    tb_lighting_prices = tb_lighting_prices.format(["country", "year", "lighting_source", "price_year"])
    tb_weeks_of_earnings = tb_weeks_of_earnings.format(
        ["country", "year"], short_name="weeks_of_earnings_needed_for_reading"
    )

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[tb_lighting_prices, tb_weeks_of_earnings], default_metadata=ds_meadow.metadata
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
    tb_lighting_prices: Table, tb_earnings: Table, tb_earnings_new: Table, tb_cpi: Table
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

    if EXPAND_EARNING_SERIES:
        tb_earnings = prepare_earnings_extended(tb_earnings_new=tb_earnings_new, tb_cpi=tb_cpi, tb_earnings=tb_earnings)

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


def prepare_earnings_extended(tb_earnings_new: Table, tb_cpi: Table, tb_earnings: Table) -> Table:
    """
    Prepare extended earnings series by rebasing new series and appending to original long-run earnings table.
    """
    # Merge tb_earning_new with tb_cpi to rebase earnings
    tb_earnings_new = pr.merge(
        tb_earnings_new,
        tb_cpi,
        on=["country", "year"],
        how="left",
    )

    # Calculate cpi_base for year 2000
    tb_earnings_new["cpi_base_new"] = tb_earnings_new.groupby("country")["cpi"].transform(
        lambda x: x.loc[tb_earnings_new["year"] == 2000].values[0]
    )

    # Calculate cpi_base for year 2015
    tb_earnings_new["cpi_base_old"] = tb_earnings_new.groupby("country")["cpi"].transform(
        lambda x: x.loc[tb_earnings_new["year"] == 2015].values[0]
    )

    # Calculate real_average_weekly_earnings as the real value in 2015 prices multiplied by the ratio of cpi_base_new to cpi_base_old
    tb_earnings_new["real_average_weekly_earnings"] = tb_earnings_new["average_weekly_earnings"] * (
        tb_earnings_new["cpi_base_new"] / tb_earnings_new["cpi_base_old"]
    )

    # Calculate the maximum year in tb_earnings
    max_year_earnings = tb_earnings["year"].max()

    # Filter tb_earnings_new for years greater than max_year_earnings
    tb_earnings_new_filtered = tb_earnings_new[tb_earnings_new["year"] > max_year_earnings][
        ["country", "year", "real_average_weekly_earnings"]
    ]

    if DEBUG:
        # Combine tb_earnings with tb_earnings_new
        tb_earnings_debug = pr.merge(
            tb_earnings[["country", "year", "real_average_weekly_earnings"]],
            tb_earnings_new[["country", "year", "real_average_weekly_earnings"]],
            on=["country", "year"],
            how="inner",
            validate="1:1",
            suffixes=("_old", "_new"),
        )
        tb_earnings_debug.to_csv("debug_earnings_overlap.csv", index=False)

        # Calculate absolute and relative differences between old and new earnings
        tb_earnings_debug["absolute_difference"] = (
            tb_earnings_debug["real_average_weekly_earnings_new"]
            - tb_earnings_debug["real_average_weekly_earnings_old"]
        ).abs()
        tb_earnings_debug["relative_difference"] = (
            tb_earnings_debug["absolute_difference"] / tb_earnings_debug["real_average_weekly_earnings_old"]
        )

        min_year = tb_earnings_debug["year"].min()
        max_year = tb_earnings_debug["year"].max()

        print(f"Earnings overlap debug info ({min_year}-{max_year}):")
        print(tb_earnings_debug[["absolute_difference", "relative_difference"]].describe())

    # Append the filtered new earnings to the original earnings table
    tb_earnings = pr.concat([tb_earnings, tb_earnings_new_filtered], ignore_index=True)

    return tb_earnings


def add_rolling_average(tb: Table) -> Table:
    """
    Add a 5-year rolling average to the table.
    """
    tb = tb.sort_values(by=["country", "year", "lighting_source", "price_year"])
    tb["lighting_price_rolling_avg"] = tb.groupby(["country", "lighting_source", "price_year"])[
        "lighting_price"
    ].transform(lambda x: x.rolling(window=5, min_periods=1).mean())
    return tb

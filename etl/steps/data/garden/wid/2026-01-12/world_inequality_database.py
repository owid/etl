"""Load a meadow dataset and create a garden dataset."""

from typing import Tuple

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define welfare types available and their new names
WELFARE_TYPES = {
    "pretax": "before tax",
    "posttax_nat": "after tax",
    "posttax_dis": "after tax disposable",
    "wealth": "wealth",
}

# Define inequality indicators and new names
INEQUALITY_INDICATORS = {
    "p0p100_gini": "gini",
    "palma_ratio": "palma_ratio",
    "s90_s10_ratio": "s90_s10_ratio",
    "s80_s20_ratio": "s80_s20_ratio",
    "s90_s50_ratio": "s90_s50_ratio",
    "p90_p10_ratio": "p90_p10_ratio",
    "p90_p50_ratio": "p90_p50_ratio",
    "p50_p10_ratio": "p50_p10_ratio",
    "p0p50_share": "share_bottom_50",
    "p50p90_share": "share_middle_40",
    "p90p100_share": "share_top_10",
    "p99p100_share": "share_top_1",
    "p99_9p100_share": "share_top_0_1",
    "p90p99_share": "share_top_90_99",
}

# Define deciles for thr and their new names
DECILES_THR = {
    "p0p10": "0",
    "p10p20": "1",
    "p20p30": "2",
    "p30p40": "3",
    "p40p50": "4",
    "p50p60": "5",
    "p60p70": "6",
    "p70p80": "7",
    "p80p90": "8",
    "p90p100": "9",
    "p99p100": "Richest 1%",
    "p99_9p100": "Richest 0.1%",
    "p99_99p100": "Richest 0.01%",
    "p99_999p100": "Richest 0.001%",
}

# Define decile for avg and share and their new names
DECILES_AVG_SHARE = {
    "p0p10": "1",
    "p10p20": "2",
    "p20p30": "3",
    "p30p40": "4",
    "p40p50": "5",
    "p50p60": "6",
    "p60p70": "7",
    "p70p80": "8",
    "p80p90": "9",
    "p90p100": "10",
    "p99p100": "Richest 1%",
    "p99_9p100": "Richest 0.1%",
    "p99_99p100": "Richest 0.01%",
    "p99_999p100": "Richest 0.001%",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("world_inequality_database")

    # Read table from meadow dataset.
    tb = ds_meadow.read("world_inequality_database")
    tb_extrapolated = ds_meadow.read("world_inequality_database_with_extrapolations")
    tb_distribution = ds_meadow.read("world_inequality_database_distribution")
    tb_distribution_extrapolated = ds_meadow.read("world_inequality_database_distribution_with_extrapolations")
    tb_fiscal = ds_meadow.read("world_inequality_database_fiscal")

    #
    # Process data.
    #

    # Combine main table with its extrapolated version.
    tb = combine_tables(tb=tb, tb_extrapolated=tb_extrapolated)
    tb_distribution = combine_tables(tb=tb_distribution, tb_extrapolated=tb_distribution_extrapolated)

    tb_inequality, tb_incomes = make_table_long_and_separate(tb=tb)

    tb_distribution = format_percentiles_table(tb=tb_distribution)

    # Add relative poverty values to inequality table
    tb_relative_poverty = add_relative_poverty(
        tb_inequality=tb_inequality, tb_incomes=tb_incomes, tb_distribution=tb_distribution
    )

    # Make shares percentages
    tb_incomes["share"] *= 100
    tb_distribution["share"] *= 100
    tb_fiscal[list(tb_fiscal.filter(like="share"))] *= 100
    tb_inequality[["share_bottom_50", "share_middle_40", "share_top_10", "share_top_1", "share_top_90_99"]] *= 100

    # Add period dimension to incomes table
    tb_incomes = add_period_dimension(tb=tb_incomes)

    # Improve table format.
    tb_inequality = tb_inequality.format(["country", "year", "welfare_type", "extrapolated"], short_name="inequality")
    tb_incomes = tb_incomes.format(
        ["country", "year", "welfare_type", "quantile", "period", "extrapolated"], short_name="incomes"
    )
    tb_relative_poverty = tb_relative_poverty.format(
        ["country", "year", "welfare_type", "poverty_line", "extrapolated"], short_name="relative_poverty"
    )
    tb_distribution = tb_distribution.format(
        ["country", "year", "welfare_type", "p", "percentile", "extrapolated"], short_name="distribution"
    )
    tb_fiscal = tb_fiscal.format(["country", "year"], short_name="fiscal_income")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[
            tb_inequality,
            tb_incomes,
            tb_relative_poverty,
            tb_distribution,
            tb_fiscal,
        ],
        default_metadata=ds_meadow.metadata,
        repack=False,
    )

    # Save garden dataset.
    ds_garden.save()


def combine_tables(tb: Table, tb_extrapolated: Table) -> Table:
    """
    Combine the main table with its extrapolated version. We concatenate and add a new column to indicate whether the value is extrapolated or not.
    """
    tb["extrapolated"] = "no"
    tb_extrapolated["extrapolated"] = "yes"

    tb_combined = pr.concat([tb, tb_extrapolated], ignore_index=True)

    return tb_combined


def make_table_long_and_separate(tb: Table) -> Tuple[Table, Table]:
    """
    Convert the table to long format, to create dimensionality for indicators.
    Also, separate the tables into two: one for inequality indicators and another for income indicators (avg, thr, share, median, mean).
    """
    tb_long = tb.copy()

    # Drop age and pop
    tb_long = tb_long.drop(columns=["age", "pop"], errors="raise")

    tb_long = tb_long.melt(id_vars=["country", "year", "extrapolated"], var_name="indicator", value_name="value")

    # Drop empty values
    tb_long = tb_long.dropna(subset=["value"]).reset_index(drop=True)

    # Extract welfare_type from indicator name (using the keys in WELFARE_TYPES) and create a new column (delete welfare_type from indicator name)
    tb_long["welfare_type"] = tb_long["indicator"].apply(
        lambda x: next((WELFARE_TYPES[key] for key in WELFARE_TYPES if key in x), None)
    )
    tb_long["indicator"] = tb_long["indicator"].apply(
        lambda x: next((x.replace(f"_{key}", "") for key in WELFARE_TYPES if key in x), x)
    )

    # Assert that all indicators have a welfare type assigned
    assert (
        tb_long["welfare_type"].isnull().sum() == 0
    ), f"Some indicators do not have a welfare type assigned: {tb_long[tb_long['welfare_type'].isnull()]['indicator'].unique()}"

    # Assert that all inequality indicators are in the INEQUALITY_INDICATORS keys
    indicators_available = set(tb_long["indicator"].unique())
    indicators_expected = set(INEQUALITY_INDICATORS.keys())
    assert indicators_expected.issubset(
        indicators_available
    ), f"Some inequality indicators are missing: {indicators_expected - indicators_available}"

    # Separate the tables between indicators not needing quantiles nor period adjustments (inequality indicators) and those needing them (share, thr, avg, median, mean)
    tb_inequality = tb_long[tb_long["indicator"].isin(INEQUALITY_INDICATORS.keys())].copy()

    # Rename inequality indicators
    tb_inequality["indicator"] = tb_inequality["indicator"].replace(INEQUALITY_INDICATORS)

    # Make the table wide with the indicators as columns.
    tb_inequality = tb_inequality.pivot_table(
        index=["country", "year", "welfare_type", "extrapolated"],
        columns="indicator",
        values="value",
    ).reset_index()

    # Create tb_incomes, which is a table with avg, thr, share, median and mean
    # Remove top 10 and top 1 from INEQUALITY_INDICATORS keys
    indicators_to_remove = INEQUALITY_INDICATORS.keys() - {"p90p100_share", "p99p100_share", "p99_9p100_share"}
    tb_incomes = tb_long[~tb_long["indicator"].isin(indicators_to_remove)].copy()

    # Rename p0p100_avg to mean
    tb_incomes["indicator"] = tb_incomes["indicator"].replace({"p0p100_avg": "mean"})

    # Separate table in tb_mean_median and tb_incomes, by selecting mean and median or not
    tb_mean_median = tb_incomes[tb_incomes["indicator"].isin(["mean", "median"])].copy()
    tb_incomes = tb_incomes[~tb_incomes["indicator"].isin(["mean", "median"])].copy()

    # Separate indicators in tb_incomes into two columns: quantile and ind, considering the last underscore as separator
    tb_incomes[["quantile", "ind"]] = tb_incomes["indicator"].str.rsplit("_", n=1, expand=True).values

    # If ind is avg or share, replace quantile values using DECILES_AVG_SHARE
    tb_incomes.loc[tb_incomes["ind"].isin(["avg", "share"]), "quantile"] = tb_incomes.loc[
        tb_incomes["ind"].isin(["avg", "share"]), "quantile"
    ].replace(DECILES_AVG_SHARE)

    # If ind is thr, replace quantile values using DECILES_THR
    tb_incomes.loc[tb_incomes["ind"] == "thr", "quantile"] = tb_incomes.loc[
        tb_incomes["ind"] == "thr", "quantile"
    ].replace(DECILES_THR)

    # Drop quantile == "0"
    tb_incomes = tb_incomes[tb_incomes["quantile"] != "0"].reset_index(drop=True)

    # Make the table wide with the indicators as columns.
    tb_incomes = tb_incomes.pivot_table(
        index=["country", "year", "welfare_type", "quantile", "extrapolated"],
        columns="ind",
        values="value",
    ).reset_index()

    # Make tb_mean_median wide as well
    tb_mean_median = tb_mean_median.pivot_table(
        index=["country", "year", "welfare_type", "extrapolated"],
        columns="indicator",
        values="value",
    ).reset_index()

    # Concatenate tb_incomes and tb_mean_median
    tb_incomes = pr.concat([tb_incomes, tb_mean_median], ignore_index=True)

    # Copy origins from original table
    for col in tb_inequality.columns:
        tb_inequality[col].m.origins = tb["p0p100_gini_pretax"].m.origins
    for col in tb_incomes.columns:
        tb_incomes[col].m.origins = tb["p0p100_gini_pretax"].m.origins

    return tb_inequality, tb_incomes


def add_period_dimension(tb: Table) -> Table:
    """
    Add period dimension to incomes table (day, month, year).
    """

    # Separate table between "non-periodicable" and "periodable" indicators
    tb_period = tb[
        ["country", "year", "welfare_type", "quantile", "extrapolated", "mean", "median", "avg", "thr"]
    ].copy()
    tb_non_period = tb[["country", "year", "welfare_type", "quantile", "extrapolated", "share"]].copy()

    # Create two copies of tb_period, one for "day" and another for "month"
    tb_day = tb_period.copy()
    tb_month = tb_period.copy()

    for col in ["mean", "median", "avg", "thr"]:
        tb_day[col] = tb_day[col] / 365
        tb_month[col] = tb_month[col] / 12

        tb_day["period"] = "day"
        tb_month["period"] = "month"

    # Define the column 'period' with value 'year' in tb_period
    tb_period["period"] = "year"

    # Concatenate all the tables
    tb = pr.concat([tb_period, tb_day, tb_month, tb_non_period], ignore_index=True, sort=False)

    return tb


def format_percentiles_table(tb: Table) -> Table:
    """
    Format the percentiles table, renaming welfare column and its categories
    """
    tb = tb.rename(columns={"welfare": "welfare_type"})

    # Assert that all welfare types are in WELFARE_TYPES keys
    welfare_types_available = set(tb["welfare_type"].unique())
    welfare_types_expected = set(WELFARE_TYPES.keys())
    assert welfare_types_expected.issubset(
        welfare_types_available
    ), f"Some welfare types are missing: {welfare_types_expected - welfare_types_available}"

    # Replace welfare types
    tb["welfare_type"] = tb["welfare_type"].replace(WELFARE_TYPES)

    return tb


def add_relative_poverty(tb_inequality: Table, tb_incomes: Table, tb_distribution: Table) -> Table:
    """
    Add relative poverty values by estimating the median and checking that value against the percentile distribution.
    Returns the inequality table with added relative poverty headcount ratio columns.
    """
    # Make copies of the tables
    tb_incomes = tb_incomes.copy()
    tb_distribution = tb_distribution.copy()
    tb_inequality = tb_inequality.copy()

    # Filter tb_incomes to only include median values
    tb_median = tb_incomes[tb_incomes["quantile"].isnull()][
        ["country", "year", "welfare_type", "extrapolated", "median"]
    ].copy()

    # Merge distribution with median values
    tb_distribution = pr.merge(
        tb_distribution,
        tb_median,
        on=["country", "year", "welfare_type", "extrapolated"],
        how="left",
    )

    # Calculate 40%, 50%, and 60% of the median for each welfare type
    for pct in [40, 50, 60]:
        tb_distribution[f"median{pct}pct"] = tb_distribution["median"] * pct / 100

        # Calculate absolute difference between thresholds and percentage of median
        tb_distribution[f"abs_diff{pct}pct"] = abs(tb_distribution["thr"] - tb_distribution[f"median{pct}pct"])

    # For each country, year, welfare_type, extrapolated combination, find the percentile with the minimum absolute difference
    tables_relative_poverty = []
    for pct in [40, 50, 60]:
        # Filter to rows with the minimum absolute difference for each group
        tb_min = tb_distribution[
            tb_distribution[f"abs_diff{pct}pct"]
            == tb_distribution.groupby(["country", "year", "welfare_type", "extrapolated"])[
                f"abs_diff{pct}pct"
            ].transform("min")
        ].copy()

        # Drop duplicates, keeping the last occurrence
        tb_min = tb_min.drop_duplicates(subset=["country", "year", "welfare_type", "extrapolated"], keep="last")

        # Select only needed columns
        tb_min = tb_min[["country", "year", "welfare_type", "extrapolated", "p"]]

        # Multiply by 100 to get the headcount ratio in percentage
        tb_min["p"] *= 100
        tb_min = tb_min.rename(columns={"p": "headcount_ratio"})

        # Add poverty line column
        tb_min["poverty_line"] = f"{pct}% of median"

        # Append to list
        tables_relative_poverty.append(tb_min)

    # Concatenate all relative poverty tables
    tb_relative_poverty = pr.concat(tables_relative_poverty, ignore_index=True)

    return tb_relative_poverty

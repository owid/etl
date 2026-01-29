"""
Load World Inequality Database meadow dataset and create a garden dataset.

NOTE: To extract the log of the process (to review sanity checks, for example), follow these steps:
    1. Define DEBUG as True.
    2. (optional) Define LONG_FORMAT as True to see the full tables in the log.
    3. Run the following command in the terminal:
        nohup .venv/bin/etlr world_inequality_database > output_wid.log 2>&1 &

"""

from typing import Tuple

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from tabulate import tabulate

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Set table format when printing
TABLEFMT = "pretty"

# Define if sanity checks should run (set to True to see sanity check output)
DEBUG = False

# Define if I show the full table or just the first 5 rows for assertions (only applies when DEBUG=True)
LONG_FORMAT = False

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

    # Sanity checks
    sanity_checks(
        tb_inequality=tb_inequality,
        tb_incomes=tb_incomes,
        tb_distribution=tb_distribution,
    )

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
        tb_min["poverty_line"] = f"{pct}% of the median"

        # Append to list
        tables_relative_poverty.append(tb_min)

    # Concatenate all relative poverty tables
    tb_relative_poverty = pr.concat(tables_relative_poverty, ignore_index=True)

    return tb_relative_poverty


def sanity_checks(tb_inequality: Table, tb_incomes: Table, tb_distribution: Table) -> None:
    """
    Perform sanity checks on the data
    """
    if not DEBUG:
        return

    check_between_0_and_1(tb_inequality)
    check_shares_sum_100(tb_incomes)
    check_negative_values(tb_inequality, tb_incomes, tb_distribution)
    check_monotonicity(tb_incomes)
    check_avg_between_thr(tb_incomes)


def check_between_0_and_1(tb_inequality: Table) -> None:
    """
    Check that gini indicators are between 0 and 1
    """
    tb = tb_inequality.reset_index()

    # Check gini values
    for welfare_type in WELFARE_TYPES.values():
        for extrapolated in ["no", "yes"]:
            mask = (tb["welfare_type"] == welfare_type) & (tb["extrapolated"] == extrapolated)
            tb_subset = tb[mask].copy()

            # Check gini
            gini_mask = (tb_subset["gini"] > 1) | (tb_subset["gini"] < 0)
            any_error = gini_mask.any()

            if any_error and welfare_type not in ["wealth", "after tax disposable"]:
                tb_error = tb_subset[gini_mask][["country", "year", "gini"]].copy()
                paths.log.fatal(
                    f"""{len(tb_error)} gini values for {welfare_type} (extrapolated={extrapolated}) are not between 0 and 1:
                    {_tabulate(tb_error)}"""
                )
            elif any_error and welfare_type in ["wealth", "after tax disposable"]:
                tb_error = tb_subset[gini_mask][["country", "year", "gini"]].copy()
                paths.log.warning(
                    f"""{len(tb_error)} gini values for {welfare_type} (extrapolated={extrapolated}) are not between 0 and 1:
                    {_tabulate(tb_error)}"""
                )


def check_shares_sum_100(tb_incomes: Table, margin: float = 0.5) -> None:
    """
    Check if the sum of decile shares is 100 (with a margin)
    """
    tb = tb_incomes.reset_index()

    # Filter to only share values for deciles 1-10 (exclude period dimension, other quantiles, wealth and after tax disposable)
    deciles = [str(i) for i in range(1, 11)]
    tb_shares = tb[
        (tb["quantile"].isin(deciles))
        & (tb["period"].isnull())
        & (~tb["welfare_type"].isin(["wealth", "after tax disposable"]))
    ].copy()

    for welfare_type in WELFARE_TYPES.values():
        # Skip wealth and after tax disposable
        if welfare_type in ["wealth", "after tax disposable"]:
            continue

        for extrapolated in ["no", "yes"]:
            # Filter by welfare type and extrapolated status
            mask = (tb_shares["welfare_type"] == welfare_type) & (tb_shares["extrapolated"] == extrapolated)
            tb_subset = tb_shares[mask].copy()

            # Calculate sum of shares for each country-year
            tb_sum = (
                tb_subset.groupby(["country", "year", "welfare_type", "extrapolated"])["share"]
                .sum()
                .reset_index(name="sum_check")
            )

            # Count how many deciles have data for each country-year
            tb_count = (
                tb_subset.groupby(["country", "year", "welfare_type", "extrapolated"])["share"]
                .count()
                .reset_index(name="count_check")
            )

            # Merge
            tb_check = pr.merge(tb_sum, tb_count, on=["country", "year", "welfare_type", "extrapolated"])

            # Only check when all 10 deciles are present
            error_mask = ((tb_check["sum_check"] >= 100 + margin) | (tb_check["sum_check"] <= 100 - margin)) & (
                tb_check["count_check"] == 10
            )
            any_error = error_mask.any()

            if any_error:
                tb_error = tb_check[error_mask][["country", "year", "sum_check"]].sort_values(
                    by="sum_check", ascending=False
                )
                paths.log.fatal(
                    f"""{len(tb_error)} share observations for {welfare_type} (extrapolated={extrapolated}) are not adding up to 100%:
                    {_tabulate(tb_error, floatfmt=".1f")}"""
                )


def check_negative_values(tb_inequality: Table, tb_incomes: Table, tb_distribution: Table) -> None:
    """
    Check if there are negative values in the variables (excluding wealth and after tax disposable)
    """
    # 1. Check tb_inequality - all numeric columns (exclude wealth, after tax disposable, and gini)
    tb = tb_inequality.reset_index()
    tb_filtered = tb[~tb["welfare_type"].isin(["wealth", "after tax disposable"])].copy()

    # Get all numeric columns excluding index columns and gini
    # Check if dtype string contains 'float' or 'int' (works with both standard and nullable dtypes)
    cols_to_check = [
        col
        for col in tb_filtered.columns
        if col not in ["country", "year", "welfare_type", "extrapolated", "gini", "index"]
        and any(dtype_str in str(tb_filtered[col].dtype).lower() for dtype_str in ["float", "int"])
    ]

    for col in cols_to_check:
        mask = tb_filtered[col] < 0
        any_error = mask.any()

        if any_error:
            tb_error = tb_filtered[mask][["country", "year", "welfare_type", "extrapolated", col]].copy()
            paths.log.warning(
                f"""{len(tb_error)} observations for {col} are negative:
                {_tabulate(tb_error)}"""
            )

    # 2. Check tb_incomes (avg, thr, share, mean, median) - exclude wealth and after tax disposable
    tb = tb_incomes.reset_index()
    tb_filtered = tb[~tb["welfare_type"].isin(["wealth", "after tax disposable"])].copy()

    # Get all numeric columns excluding index columns
    cols_to_check = [
        col
        for col in tb_filtered.columns
        if col not in ["country", "year", "welfare_type", "extrapolated", "quantile", "period", "index"]
        and any(dtype_str in str(tb_filtered[col].dtype).lower() for dtype_str in ["float", "int"])
    ]

    for col in cols_to_check:
        mask = tb_filtered[col] < 0
        any_error = mask.any()

        if any_error:
            # Include quantile in output if present
            cols_to_show = ["country", "year", "welfare_type", "extrapolated"]
            if "quantile" in tb_filtered.columns:
                cols_to_show.append("quantile")
            cols_to_show.append(col)

            tb_error = tb_filtered[mask][cols_to_show].copy()
            paths.log.warning(
                f"""{len(tb_error)} observations for {col} are negative:
                {_tabulate(tb_error)}"""
            )

    # 3. Check tb_distribution (thr, share) - exclude wealth and after tax disposable
    tb = tb_distribution.reset_index()
    tb_filtered = tb[~tb["welfare_type"].isin(["wealth", "after tax disposable"])].copy()

    # Get all numeric columns excluding index columns
    cols_to_check = [
        col
        for col in tb_filtered.columns
        if col not in ["country", "year", "welfare_type", "extrapolated", "p", "percentile", "index"]
        and any(dtype_str in str(tb_filtered[col].dtype).lower() for dtype_str in ["float", "int"])
    ]

    for col in cols_to_check:
        mask = tb_filtered[col] < 0
        any_error = mask.any()

        if any_error:
            tb_error = tb_filtered[mask][["country", "year", "welfare_type", "percentile", "extrapolated", col]].copy()
            paths.log.warning(
                f"""{len(tb_error)} observations for {col} in distribution are negative:
                {_tabulate(tb_error)}"""
            )


def check_monotonicity(tb_incomes: Table) -> None:
    """
    Check monotonicity for shares, thresholds and averages across deciles
    """
    tb = tb_incomes.reset_index()

    for welfare_type in WELFARE_TYPES.values():
        for extrapolated in ["no", "yes"]:
            for metric in ["avg", "thr", "share"]:
                # thr uses quantiles 1-9, avg and share use quantiles 1-10
                if metric == "thr":
                    deciles = [str(i) for i in range(1, 10)]
                else:
                    deciles = [str(i) for i in range(1, 11)]

                # Filter to the right deciles
                # For avg and thr, use period="year"; for share, use period=null
                if metric in ["avg", "thr"]:
                    tb_deciles = tb[(tb["quantile"].isin(deciles)) & (tb["period"] == "year")].copy()
                else:  # share
                    tb_deciles = tb[(tb["quantile"].isin(deciles)) & (tb["period"].isnull())].copy()

                # Filter by welfare type and extrapolated status
                mask = (tb_deciles["welfare_type"] == welfare_type) & (tb_deciles["extrapolated"] == extrapolated)
                tb_subset = tb_deciles[mask].copy()

                # Skip if no data for this combination
                if tb_subset.empty or metric not in tb_subset.columns:
                    continue

                # Sort by country, year, and quantile (as integers for proper ordering)
                tb_subset["quantile_int"] = tb_subset["quantile"].astype(int)
                tb_subset = tb_subset.sort_values(["country", "year", "quantile_int"])

                # First, ensure each country-year has ALL expected rows (not just non-null values)
                # Count rows per country-year and filter to only those with the expected number
                expected_row_count = len(deciles)
                tb_subset["row_count"] = tb_subset.groupby(["country", "year"])["quantile"].transform("count")
                tb_subset = tb_subset[tb_subset["row_count"] == expected_row_count].copy()

                if tb_subset.empty:
                    continue

                # Now count non-null values per country-year group to only check complete sets
                tb_subset["null_count"] = tb_subset.groupby(["country", "year"])[metric].transform(
                    lambda x: x.isnull().sum()
                )

                # Only check country-years where ALL deciles have non-null values (like legacy version)
                tb_complete = tb_subset[tb_subset["null_count"] == 0].copy()

                if tb_complete.empty:
                    continue

                # Get previous value within each country-year group
                tb_complete["prev_value"] = tb_complete.groupby(["country", "year"])[metric].shift(1)
                tb_complete["prev_quantile"] = tb_complete.groupby(["country", "year"])["quantile"].shift(1)

                # Check monotonicity: current value should be >= previous value
                # Only check where we have both current and previous values
                tb_complete["is_monotonic"] = (tb_complete[metric] >= tb_complete["prev_value"]) | tb_complete[
                    "prev_value"
                ].isnull()

                # Find violations (excluding first decile in each group which has no previous)
                errors = tb_complete[~tb_complete["is_monotonic"] & tb_complete["prev_value"].notnull()].copy()

                if not errors.empty:
                    # Format error output
                    errors["error_desc"] = (
                        "Decile "
                        + errors["prev_quantile"]
                        + " ("
                        + errors["prev_value"].round(4).astype(str)
                        + ") -> Decile "
                        + errors["quantile"]
                        + " ("
                        + errors[metric].round(4).astype(str)
                        + ")"
                    )

                    tb_error = errors[["country", "year", "error_desc"]].copy()
                    paths.log.fatal(
                        f"""{len(tb_error)} observations for {metric} {welfare_type} (extrapolated={extrapolated}) are not monotonically increasing:
                        {_tabulate(tb_error)}"""
                    )


def check_avg_between_thr(tb_incomes: Table) -> None:
    """
    Check that each avg is between the corresponding thr boundaries
    Note: avg uses quantiles 1-10, thr uses quantiles 1-9
    avg quantile i should be between thr quantile i and thr quantile (i+1)
    For avg quantile 9 and 10, lower bound is thr 9, no upper bound
    """
    tb = tb_incomes.reset_index()

    for welfare_type in WELFARE_TYPES.values():
        for extrapolated in ["no", "yes"]:
            # Get avg data (quantiles 1-10, period="year")
            avg_deciles = [str(i) for i in range(1, 11)]
            tb_avg = tb[
                (tb["quantile"].isin(avg_deciles))
                & (tb["period"] == "year")
                & (tb["welfare_type"] == welfare_type)
                & (tb["extrapolated"] == extrapolated)
            ].copy()

            # Get thr data (quantiles 1-9, period="year")
            thr_deciles = [str(i) for i in range(1, 10)]
            tb_thr = tb[
                (tb["quantile"].isin(thr_deciles))
                & (tb["period"] == "year")
                & (tb["welfare_type"] == welfare_type)
                & (tb["extrapolated"] == extrapolated)
            ].copy()

            # Skip if no data
            if tb_avg.empty or tb_thr.empty or "avg" not in tb_avg.columns or "thr" not in tb_thr.columns:
                continue

            # First, count rows per country-year to ensure all deciles are present
            avg_row_counts = tb_avg.groupby(["country", "year"])["quantile"].count().reset_index(name="row_count_avg")
            thr_row_counts = tb_thr.groupby(["country", "year"])["quantile"].count().reset_index(name="row_count_thr")

            # Filter to country-years with all expected rows
            complete_avg_rows = avg_row_counts[avg_row_counts["row_count_avg"] == 10][["country", "year"]]
            complete_thr_rows = thr_row_counts[thr_row_counts["row_count_thr"] == 9][["country", "year"]]

            # Now count non-null values to only check complete sets
            avg_nulls = (
                tb_avg.groupby(["country", "year"])["avg"]
                .apply(lambda x: x.isnull().sum())
                .reset_index(name="null_count_avg")
            )
            thr_nulls = (
                tb_thr.groupby(["country", "year"])["thr"]
                .apply(lambda x: x.isnull().sum())
                .reset_index(name="null_count_thr")
            )

            # Find country-years with complete data (all deciles present AND non-null)
            complete_avg = pr.merge(
                complete_avg_rows,
                avg_nulls[avg_nulls["null_count_avg"] == 0][["country", "year"]],
                on=["country", "year"],
            )
            complete_thr = pr.merge(
                complete_thr_rows,
                thr_nulls[thr_nulls["null_count_thr"] == 0][["country", "year"]],
                on=["country", "year"],
            )
            complete_both = pr.merge(complete_avg, complete_thr, on=["country", "year"])

            if complete_both.empty:
                continue

            # Filter to only complete country-years
            tb_avg_complete = pr.merge(tb_avg, complete_both, on=["country", "year"])[
                ["country", "year", "quantile", "avg"]
            ]
            tb_thr_complete = pr.merge(tb_thr, complete_both, on=["country", "year"])[
                ["country", "year", "quantile", "thr"]
            ]

            # Create integer quantile for sorting/joining
            tb_avg_complete["quantile_int"] = tb_avg_complete["quantile"].astype(int)
            tb_thr_complete["quantile_int"] = tb_thr_complete["quantile"].astype(int)

            # For each avg quantile i (1-10), check boundaries:
            # Note: Due to quantile mapping offset:
            #   avg quantile i corresponds to p{(i-1)*10}p{i*10}
            #   thr quantile j corresponds to p{j*10}p{(j+1)*10}
            # So avg quantile i should be between thr quantile (i-1) and thr quantile i
            # But thr quantile 0 is dropped, so:
            #   avg quantile 1 -> only upper bound: thr quantile 1
            #   avg quantile 2 -> thr quantile 1 (lower) and thr quantile 2 (upper)
            #   ...
            #   avg quantile 9 -> thr quantile 8 (lower) and thr quantile 9 (upper)
            #   avg quantile 10 -> only lower bound: thr quantile 9

            errors_list = []
            for avg_q in range(1, 11):
                tb_avg_q = tb_avg_complete[tb_avg_complete["quantile_int"] == avg_q].copy()

                # Lower bound is thr quantile (avg_q - 1), but thr quantile 0 doesn't exist
                # So only check lower bound for avg_q >= 2
                if avg_q >= 2:
                    thr_lower_q = avg_q - 1
                    tb_thr_lower = tb_thr_complete[tb_thr_complete["quantile_int"] == thr_lower_q].copy()
                    tb_thr_lower = tb_thr_lower[["country", "year", "thr"]].rename(columns={"thr": "thr_lower"})

                    # Merge to get lower bound
                    tb_check = pr.merge(
                        tb_avg_q,
                        tb_thr_lower,
                        on=["country", "year"],
                    )
                else:
                    # For avg quantile 1, no lower bound (thr quantile 0 is dropped)
                    tb_check = tb_avg_q.copy()
                    tb_check["thr_lower"] = None

                # Upper bound is thr quantile avg_q (but thr only goes up to 9)
                if avg_q <= 9:
                    thr_upper_q = avg_q
                    tb_thr_upper = tb_thr_complete[tb_thr_complete["quantile_int"] == thr_upper_q].copy()
                    tb_thr_upper = tb_thr_upper[["country", "year", "thr"]].rename(columns={"thr": "thr_upper"})
                    tb_check = pr.merge(
                        tb_check,
                        tb_thr_upper,
                        on=["country", "year"],
                    )

                    # Check validity
                    if avg_q == 1:
                        # Only upper bound: avg <= thr_upper
                        tb_check["is_valid"] = tb_check["avg"] <= tb_check["thr_upper"]
                    else:
                        # Both bounds: thr_lower <= avg <= thr_upper
                        tb_check["is_valid"] = (tb_check["avg"] >= tb_check["thr_lower"]) & (
                            tb_check["avg"] <= tb_check["thr_upper"]
                        )
                else:
                    # For avg quantile 10, only lower bound: avg >= thr_lower
                    tb_check["thr_upper"] = None
                    tb_check["is_valid"] = tb_check["avg"] >= tb_check["thr_lower"]

                # Find violations
                violations = tb_check[~tb_check["is_valid"]].copy()
                if not violations.empty:
                    errors_list.append(violations)

            if errors_list:
                errors = pr.concat(errors_list, ignore_index=True)

                # Format error output
                errors["thr_lower_str"] = (
                    errors["thr_lower"].round(4).astype(str)
                    if "thr_lower" in errors.columns
                    else errors["thr"].round(4).astype(str)
                )
                errors["thr_upper_str"] = errors["thr_upper"].apply(
                    lambda x: f"{x:.4f}" if pd.notnull(x) else "N/A (last decile)"
                )

                errors["error_desc"] = (
                    "Decile "
                    + errors["quantile"]
                    + ": avg="
                    + errors["avg"].round(4).astype(str)
                    + ", thr_lower="
                    + errors["thr_lower_str"]
                    + ", thr_upper="
                    + errors["thr_upper_str"]
                )

                tb_error = errors[["country", "year", "error_desc"]].copy()
                paths.log.fatal(
                    f"""{len(tb_error)} observations for avg {welfare_type} (extrapolated={extrapolated}) are not between the corresponding thresholds:
                    {_tabulate(tb_error)}"""
                )


def _tabulate(tb: Table, headers="keys", tablefmt=TABLEFMT, **kwargs):
    """Helper function to format tables for display"""
    if LONG_FORMAT:
        return tabulate(tb, headers=headers, tablefmt=tablefmt, **kwargs)
    else:
        return tabulate(tb.head(5), headers=headers, tablefmt=tablefmt, **kwargs)

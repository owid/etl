"""
Load a meadow dataset and create a garden dataset.

NOTE: To extract the log of the process (to review sanity checks, for example), follow these steps:
    1. Define DEBUG as True.
    2. (optional) Define LONG_FORMAT as True to see the full tables in the log.
    3. Run the following command in the terminal:
        nohup .venv/bin/etlr luxembourg_income_study > output_lis.log 2>&1 &

"""

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
DEBUG = True

# Define if I show the full table or just the first 5 rows for assertions (only applies when DEBUG=True)
LONG_FORMAT = False

# Define absolute poverty columns and new names
ABSOLUTE_POVERTY_COLUMNS = {
    "absolute_poverty_rate": "headcount_ratio",
    "number_poor_abs": "headcount",
    "absolute_poverty_gap_index": "poverty_gap_index",
    "average_poverty_shortfall_abs": "avg_shortfall",
    "percentage_poverty_shortfall_abs": "income_gap_ratio",
    "total_shortfall_abs": "total_shortfall",
}

# Define relative poverty columns and new names
RELATIVE_POVERTY_COLUMNS = {
    "relative_poverty_rate": "headcount_ratio",
    "number_poor_relative": "headcount",
    "relative_poverty_gap_index": "poverty_gap_index",
    "average_poverty_shortfall_relt_to_median": "avg_shortfall",
    "percentage_poverty_shortfall_relt_to_median": "income_gap_ratio",
    "total_shortfall_relative": "total_shortfall",
}

# Define World Bank poverty lines (to show price in the right format)
WORLD_BANK_POVERTY_LINES = {
    "3": "3",
    "4.2": "4.20",
    "8.3": "8.30",
}

# Define relative poverty lines
RELATIVE_POVERTY_LINES = {
    "0.4": "40% of the median",
    "0.5": "50% of the median",
    "0.6": "60% of the median",
}

# Define inequality indicators and new names
INEQUALITY_INDICATORS = {
    "Gini Index": "gini",
    "Palma Ratio": "palma_ratio",
    "Ratio p50_p10": "p50_p10_ratio",
    "Ratio p90_p10": "p90_p10_ratio",
    "Ratio p90_p50": "p90_p50_ratio",
    "Share Bottom 50": "share_bottom_50",
    "Share Top 10": "share_top_10",
    "Share below half median": "share_below_50pct_median",
}

# Define income indicators and new names
INCOME_INDICATORS = {
    "Average": "mean",
    "Median": "median",
    "d_10": "thr_1",
    "d_20": "thr_2",
    "d_30": "thr_3",
    "d_40": "thr_4",
    "d_50": "thr_5",
    "d_60": "thr_6",
    "d_70": "thr_7",
    "d_80": "thr_8",
    "d_90": "thr_9",
    "decile_averages_p_0-10": "avg_1",
    "decile_averages_p_10-20": "avg_2",
    "decile_averages_p_20-30": "avg_3",
    "decile_averages_p_30-40": "avg_4",
    "decile_averages_p_40-50": "avg_5",
    "decile_averages_p_50-60": "avg_6",
    "decile_averages_p_60-70": "avg_7",
    "decile_averages_p_70-80": "avg_8",
    "decile_averages_p_80-90": "avg_9",
    "decile_averages_p_90-100": "avg_10",
    "decile_shares_p_0-10": "share_1",
    "decile_shares_p_10-20": "share_2",
    "decile_shares_p_20-30": "share_3",
    "decile_shares_p_30-40": "share_4",
    "decile_shares_p_40-50": "share_5",
    "decile_shares_p_50-60": "share_6",
    "decile_shares_p_60-70": "share_7",
    "decile_shares_p_70-80": "share_8",
    "decile_shares_p_80-90": "share_9",
    "decile_shares_p_90-100": "share_10",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("luxembourg_income_study")

    # Read table from meadow dataset.
    tb_absolute_poverty = ds_meadow.read("absolute_poverty")
    tb_incomes = ds_meadow.read("incomes")
    tb_inequality = ds_meadow.read("inequality")
    tb_relative_poverty = ds_meadow.read("relative_poverty")

    #
    # Process data.
    #
    # Harmonize country names.
    tb_absolute_poverty = paths.regions.harmonize_names(tb=tb_absolute_poverty, warn_on_unused_countries=False)
    tb_incomes = paths.regions.harmonize_names(tb=tb_incomes, warn_on_unused_countries=False)
    tb_inequality = paths.regions.harmonize_names(tb=tb_inequality)
    tb_relative_poverty = paths.regions.harmonize_names(tb=tb_relative_poverty)

    tb_absolute_poverty = process_poverty(tb=tb_absolute_poverty, absolute=True)
    tb_relative_poverty = process_poverty(tb=tb_relative_poverty, absolute=False)
    tb_inequality = process_inequality(tb=tb_inequality)
    tb_incomes = process_incomes(tb=tb_incomes)

    tb_incomes = add_period_dimension(tb=tb_incomes)

    # Concatenate poverty tables into one
    tb_poverty = pr.concat(
        [tb_absolute_poverty, tb_relative_poverty],
        ignore_index=True,
        sort=False,
    )

    # Sanity checks
    sanity_checks(
        tb_inequality=tb_inequality,
        tb_incomes=tb_incomes,
        tb_poverty=tb_poverty,
    )

    # Improve table format.
    tb_poverty = tb_poverty.format(
        ["country", "year", "poverty_line", "welfare_type", "equivalence_scale"], short_name="poverty"
    )
    tb_incomes = tb_incomes.format(
        ["country", "year", "welfare_type", "equivalence_scale", "decile", "period"], short_name="incomes"
    )
    tb_inequality = tb_inequality.format(["country", "year", "welfare_type", "equivalence_scale"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[
            tb_poverty,
            tb_incomes,
            tb_inequality,
        ],
        default_metadata=ds_meadow.metadata,
    )

    # Save garden dataset.
    ds_garden.save()


def process_poverty(tb: Table, absolute: bool) -> Table:
    """
    Process absolute and relative poverty tables.
    Extract poverty lines from indicator names and make the table wide, renaming indicators accordingly.
    """

    # Extract poverty lines from indicator names, as the last number after the last underscore. Make it integer
    tb["poverty_line"] = tb["indicator"].str.split("_").str[-1].astype(str)

    if absolute:
        column_dict = ABSOLUTE_POVERTY_COLUMNS

        # Assert the lines in WORLD_BANK_POVERTY_LINES keys are all in tb['poverty_line']
        tb_lines_expected = set(WORLD_BANK_POVERTY_LINES.keys())
        tb_lines_actual = set(tb["poverty_line"].unique())
        assert tb_lines_expected.issubset(
            tb_lines_actual
        ), f"Missing poverty lines: {tb_lines_expected - tb_lines_actual}"

        # Rename poverty lines
        tb["poverty_line"] = tb["poverty_line"].replace(WORLD_BANK_POVERTY_LINES)

    else:
        column_dict = RELATIVE_POVERTY_COLUMNS

        # Assert that all poverty lines are in RELATIVE_POVERTY_LINES keys
        poverty_lines_expected = set(RELATIVE_POVERTY_LINES.keys())
        poverty_lines_actual = set(tb["poverty_line"].unique())
        assert poverty_lines_actual.issubset(
            poverty_lines_expected
        ), f"Unexpected poverty lines: {poverty_lines_actual - poverty_lines_expected}"

        # Rename poverty lines
        tb["poverty_line"] = tb["poverty_line"].replace(RELATIVE_POVERTY_LINES)

    # Rename indicators to remove poverty line information
    tb["indicator"] = tb["indicator"].str.rsplit("_", n=1).str[0]

    # Assert that all indicators are in the ABSOLUTE_POVERTY_COLUMNS keys
    indicators_expected = set(column_dict.keys())
    indicators_actual = set(tb["indicator"].unique())
    assert indicators_actual == indicators_expected, f"Unexpected indicators: {indicators_actual - indicators_expected}"

    # Rename indicators according to ABSOLUTE_POVERTY_COLUMNS
    tb["indicator"] = tb["indicator"].replace(column_dict)

    # Pivot the table to make it wide, with indicators as columns
    tb_pivot = tb.pivot_table(
        index=["country", "year", "poverty_line", "welfare_type", "equivalence_scale"],
        columns="indicator",
        values="value",
    ).reset_index()

    # Recover origins from tb
    for col in tb_pivot.columns:
        tb_pivot[col].m.origins = tb["value"].m.origins

    return tb_pivot


def process_inequality(tb: Table) -> Table:
    """
    Process inequality table.
    Rename indicators accordingly.
    """
    # Assert that all indicators are in the INEQUALITY_INDICATORS keys
    indicators_expected = set(INEQUALITY_INDICATORS.keys())
    indicators_actual = set(tb["indicator"].unique())
    assert indicators_actual == indicators_expected, f"Unexpected indicators: {indicators_actual - indicators_expected}"

    # Rename indicators according to INEQUALITY_INDICATORS
    tb["indicator"] = tb["indicator"].replace(INEQUALITY_INDICATORS)

    # Pivot the table to make it wide, with indicators as columns
    tb_pivot = tb.pivot_table(
        index=["country", "year", "welfare_type", "equivalence_scale"],
        columns="indicator",
        values="value",
    ).reset_index()

    # Calculate share_middle_40 as 100 - share_bottom_50 - share_top_10
    tb_pivot["share_middle_40"] = 100 - tb_pivot["share_bottom_50"] - tb_pivot["share_top_10"]

    # Recover origins from tb
    for col in tb_pivot.columns:
        tb_pivot[col].m.origins = tb["value"].m.origins

    return tb_pivot


def process_incomes(tb: Table) -> Table:
    """
    Process incomes table.
    Rename indicators accordingly.
    """
    # Assert that all indicators are in the INCOME_INDICATORS keys
    indicators_expected = set(INCOME_INDICATORS.keys())
    indicators_actual = set(tb["indicator"].unique())
    assert indicators_actual == indicators_expected, f"Unexpected indicators: {indicators_actual - indicators_expected}"

    # Rename indicators according to INCOME_INDICATORS
    tb["indicator"] = tb["indicator"].replace(INCOME_INDICATORS)

    # Separate the table in two, one for mean and median, and another for the rest of indicators
    tb_mean_median = tb[tb["indicator"].isin(["mean", "median"])].copy()
    tb_deciles = tb[~tb["indicator"].isin(["mean", "median"])].copy()

    # Extract decile number from the last number in the indicator name, after the last underscore
    tb_deciles["decile"] = tb_deciles["indicator"].str.split("_").str[-1].astype(int)

    # Rename indicator to remove decile number
    tb_deciles["indicator"] = tb_deciles["indicator"].str.rsplit("_", n=1).str[0]

    # Pivot the tables to make them wide, with indicators as columns
    tb_mean_median_pivot = tb_mean_median.pivot_table(
        index=["country", "year", "welfare_type", "equivalence_scale"],
        columns="indicator",
        values="value",
    ).reset_index()

    tb_deciles_pivot = tb_deciles.pivot_table(
        index=["country", "year", "welfare_type", "equivalence_scale", "decile"],
        columns="indicator",
        values="value",
    ).reset_index()

    # Recover origins from tb
    for col in tb_mean_median_pivot.columns:
        tb_mean_median_pivot[col].m.origins = tb["value"].m.origins
    for col in tb_deciles_pivot.columns:
        tb_deciles_pivot[col].m.origins = tb["value"].m.origins

    # Concatenate both tables
    tb_incomes = pr.concat([tb_deciles_pivot, tb_mean_median_pivot], ignore_index=True, sort=False)

    return tb_incomes


def add_period_dimension(tb: Table) -> Table:
    """
    Add period dimension to incomes table (day, month, year).
    """

    # Separate table between "non-periodicable" and "periodable" indicators
    tb_period = tb[
        ["country", "year", "welfare_type", "equivalence_scale", "decile", "mean", "median", "avg", "thr"]
    ].copy()
    tb_non_period = tb[["country", "year", "welfare_type", "equivalence_scale", "decile", "share"]].copy()

    # Create two copues of tb_period, one for "day" and another for "month"
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


def sanity_checks(tb_inequality: Table, tb_incomes: Table, tb_poverty: Table) -> None:
    """
    Perform sanity checks on the data
    """
    if not DEBUG:
        return

    check_between_0_and_1(tb_inequality)
    check_shares_sum_100(tb_incomes)
    check_negative_values(tb_inequality, tb_incomes)
    check_monotonicity(tb_incomes)
    check_avg_between_thr(tb_incomes)
    check_poverty_range(tb_poverty)
    check_poverty_monotonicity(tb_poverty)


def check_between_0_and_1(tb_inequality: Table) -> None:
    """
    Check that gini indicators are between 0 and 1
    """
    tb = tb_inequality.reset_index()

    # Check gini values for all welfare_type and equivalence_scale combinations
    for welfare_type in tb["welfare_type"].unique():
        for equivalence_scale in tb["equivalence_scale"].unique():
            mask = (tb["welfare_type"] == welfare_type) & (tb["equivalence_scale"] == equivalence_scale)
            tb_subset = tb[mask].copy()

            # Check gini
            gini_mask = (tb_subset["gini"] > 1) | (tb_subset["gini"] < 0)
            any_error = gini_mask.any()

            if any_error:
                tb_error = tb_subset[gini_mask][["country", "year", "gini"]].copy()
                paths.log.fatal(
                    f"""{len(tb_error)} gini values for {welfare_type} ({equivalence_scale}) are not between 0 and 1:
                    {_tabulate(tb_error)}"""
                )


def check_shares_sum_100(tb_incomes: Table, margin: float = 0.5) -> None:
    """
    Check if the sum of decile shares is 100 (with a margin)
    """
    tb = tb_incomes.reset_index()

    # Filter to only share values for deciles 1-10 (exclude period dimension)
    deciles = list(range(1, 11))
    tb_shares = tb[(tb["decile"].isin(deciles)) & (tb["period"].isnull())].copy()

    for welfare_type in tb_shares["welfare_type"].unique():
        for equivalence_scale in tb_shares["equivalence_scale"].unique():
            # Filter by welfare type and equivalence scale
            mask = (tb_shares["welfare_type"] == welfare_type) & (tb_shares["equivalence_scale"] == equivalence_scale)
            tb_subset = tb_shares[mask].copy()

            # Calculate sum of shares for each country-year
            tb_sum = (
                tb_subset.groupby(["country", "year", "welfare_type", "equivalence_scale"])["share"]
                .sum()
                .reset_index(name="sum_check")
            )

            # Count how many deciles have data for each country-year
            tb_count = (
                tb_subset.groupby(["country", "year", "welfare_type", "equivalence_scale"])["share"]
                .count()
                .reset_index(name="count_check")
            )

            # Merge
            tb_check = pr.merge(tb_sum, tb_count, on=["country", "year", "welfare_type", "equivalence_scale"])

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
                    f"""{len(tb_error)} share observations for {welfare_type} ({equivalence_scale}) are not adding up to 100%:
                    {_tabulate(tb_error, floatfmt=".1f")}"""
                )


def check_negative_values(tb_inequality: Table, tb_incomes: Table) -> None:
    """
    Check if there are negative values in the variables (excluding gini)
    """
    # 1. Check tb_inequality - all numeric columns (exclude gini)
    tb = tb_inequality.reset_index()

    # Get all numeric columns excluding index columns and gini
    cols_to_check = [
        col
        for col in tb.columns
        if col not in ["country", "year", "welfare_type", "equivalence_scale", "gini", "index"]
        and any(dtype_str in str(tb[col].dtype).lower() for dtype_str in ["float", "int"])
    ]

    for col in cols_to_check:
        mask = tb[col] < 0
        any_error = mask.any()

        if any_error:
            tb_error = tb[mask][["country", "year", "welfare_type", "equivalence_scale", col]].copy()
            paths.log.warning(
                f"""{len(tb_error)} observations for {col} are negative:
                {_tabulate(tb_error)}"""
            )

    # 2. Check tb_incomes (avg, thr, share, mean, median)
    tb = tb_incomes.reset_index()

    # Get all numeric columns excluding index columns
    cols_to_check = [
        col
        for col in tb.columns
        if col not in ["country", "year", "welfare_type", "equivalence_scale", "decile", "period", "index"]
        and any(dtype_str in str(tb[col].dtype).lower() for dtype_str in ["float", "int"])
    ]

    for col in cols_to_check:
        mask = tb[col] < 0
        any_error = mask.any()

        if any_error:
            # Include decile in output if present
            cols_to_show = ["country", "year", "welfare_type", "equivalence_scale"]
            if "decile" in tb.columns:
                cols_to_show.append("decile")
            cols_to_show.append(col)

            tb_error = tb[mask][cols_to_show].copy()
            paths.log.warning(
                f"""{len(tb_error)} observations for {col} are negative:
                {_tabulate(tb_error)}"""
            )


def check_monotonicity(tb_incomes: Table) -> None:
    """
    Check monotonicity for shares, thresholds and averages across deciles
    """
    tb = tb_incomes.reset_index()

    for welfare_type in tb["welfare_type"].unique():
        for equivalence_scale in tb["equivalence_scale"].unique():
            for metric in ["avg", "thr", "share"]:
                # All metrics use deciles 1-10
                deciles = list(range(1, 11))

                # Filter to the right deciles
                # For avg and thr, use period="year"; for share, use period=null
                if metric in ["avg", "thr"]:
                    tb_deciles = tb[(tb["decile"].isin(deciles)) & (tb["period"] == "year")].copy()
                else:  # share
                    tb_deciles = tb[(tb["decile"].isin(deciles)) & (tb["period"].isnull())].copy()

                # Filter by welfare type and equivalence scale
                mask = (tb_deciles["welfare_type"] == welfare_type) & (
                    tb_deciles["equivalence_scale"] == equivalence_scale
                )
                tb_subset = tb_deciles[mask].copy()

                # Skip if no data for this combination
                if tb_subset.empty or metric not in tb_subset.columns:
                    continue

                # Sort by country, year, and decile
                tb_subset = tb_subset.sort_values(["country", "year", "decile"])

                # First, ensure each country-year has ALL expected rows (not just non-null values)
                # Count rows per country-year and filter to only those with the expected number
                expected_row_count = len(deciles)
                tb_subset["row_count"] = tb_subset.groupby(["country", "year"])["decile"].transform("count")
                tb_subset = tb_subset[tb_subset["row_count"] == expected_row_count].copy()

                if tb_subset.empty:
                    continue

                # Now count non-null values per country-year group to only check complete sets
                tb_subset["null_count"] = tb_subset.groupby(["country", "year"])[metric].transform(
                    lambda x: x.isnull().sum()
                )

                # Only check country-years where ALL deciles have non-null values
                tb_complete = tb_subset[tb_subset["null_count"] == 0].copy()

                if tb_complete.empty:
                    continue

                # Get previous value within each country-year group
                tb_complete["prev_value"] = tb_complete.groupby(["country", "year"])[metric].shift(1)
                tb_complete["prev_decile"] = tb_complete.groupby(["country", "year"])["decile"].shift(1)

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
                        + errors["prev_decile"].astype(str)
                        + " ("
                        + errors["prev_value"].round(4).astype(str)
                        + ") -> Decile "
                        + errors["decile"].astype(str)
                        + " ("
                        + errors[metric].round(4).astype(str)
                        + ")"
                    )

                    tb_error = errors[["country", "year", "error_desc"]].copy()
                    paths.log.fatal(
                        f"""{len(tb_error)} observations for {metric} {welfare_type} ({equivalence_scale}) are not monotonically increasing:
                        {_tabulate(tb_error)}"""
                    )


def check_avg_between_thr(tb_incomes: Table) -> None:
    """
    Check that each avg is between the corresponding thr boundaries
    Note: Both avg and thr use deciles 1-10
    avg decile i should be between thr decile (i-1) and thr decile i
    But thr decile 0 doesn't exist, so:
      avg decile 1 -> only upper bound: thr decile 1
      avg decile 2-10 -> thr decile (i-1) (lower) and thr decile i (upper)
    """
    tb = tb_incomes.reset_index()

    for welfare_type in tb["welfare_type"].unique():
        for equivalence_scale in tb["equivalence_scale"].unique():
            # Get avg data (deciles 1-10, period="year")
            avg_deciles = list(range(1, 11))
            tb_avg = tb[
                (tb["decile"].isin(avg_deciles))
                & (tb["period"] == "year")
                & (tb["welfare_type"] == welfare_type)
                & (tb["equivalence_scale"] == equivalence_scale)
            ].copy()

            # Get thr data (deciles 1-10, period="year")
            thr_deciles = list(range(1, 11))
            tb_thr = tb[
                (tb["decile"].isin(thr_deciles))
                & (tb["period"] == "year")
                & (tb["welfare_type"] == welfare_type)
                & (tb["equivalence_scale"] == equivalence_scale)
            ].copy()

            # Skip if no data
            if tb_avg.empty or tb_thr.empty or "avg" not in tb_avg.columns or "thr" not in tb_thr.columns:
                continue

            # First, count rows per country-year to ensure all deciles are present
            avg_row_counts = tb_avg.groupby(["country", "year"])["decile"].count().reset_index(name="row_count_avg")
            thr_row_counts = tb_thr.groupby(["country", "year"])["decile"].count().reset_index(name="row_count_thr")

            # Filter to country-years with all expected rows
            complete_avg_rows = avg_row_counts[avg_row_counts["row_count_avg"] == 10][["country", "year"]]
            complete_thr_rows = thr_row_counts[thr_row_counts["row_count_thr"] == 10][["country", "year"]]

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
                ["country", "year", "decile", "avg"]
            ]
            tb_thr_complete = pr.merge(tb_thr, complete_both, on=["country", "year"])[
                ["country", "year", "decile", "thr"]
            ]

            # For each avg decile i (1-10), check boundaries:
            # avg decile 1 -> only upper bound: thr decile 1
            # avg decile 2 -> thr decile 1 (lower) and thr decile 2 (upper)
            # ...
            # avg decile 10 -> thr decile 9 (lower) and thr decile 10 (upper)

            errors_list = []
            for avg_d in range(1, 11):
                tb_avg_d = tb_avg_complete[tb_avg_complete["decile"] == avg_d].copy()

                # Lower bound is thr decile (avg_d - 1), but decile 0 doesn't exist
                # So only check lower bound for avg_d >= 2
                if avg_d >= 2:
                    thr_lower_d = avg_d - 1
                    tb_thr_lower = tb_thr_complete[tb_thr_complete["decile"] == thr_lower_d].copy()
                    tb_thr_lower = tb_thr_lower[["country", "year", "thr"]].rename(columns={"thr": "thr_lower"})

                    # Merge to get lower bound
                    tb_check = pr.merge(
                        tb_avg_d,
                        tb_thr_lower,
                        on=["country", "year"],
                    )
                else:
                    # For avg decile 1, no lower bound (thr decile 0 doesn't exist)
                    tb_check = tb_avg_d.copy()
                    tb_check["thr_lower"] = None

                # Upper bound is thr decile avg_d
                thr_upper_d = avg_d
                tb_thr_upper = tb_thr_complete[tb_thr_complete["decile"] == thr_upper_d].copy()
                tb_thr_upper = tb_thr_upper[["country", "year", "thr"]].rename(columns={"thr": "thr_upper"})
                tb_check = pr.merge(
                    tb_check,
                    tb_thr_upper,
                    on=["country", "year"],
                )

                # Check validity
                if avg_d == 1:
                    # Only upper bound: avg <= thr_upper
                    tb_check["is_valid"] = tb_check["avg"] <= tb_check["thr_upper"]
                else:
                    # Both bounds: thr_lower <= avg <= thr_upper
                    tb_check["is_valid"] = (tb_check["avg"] >= tb_check["thr_lower"]) & (
                        tb_check["avg"] <= tb_check["thr_upper"]
                    )

                # Find violations
                violations = tb_check[~tb_check["is_valid"]].copy()
                if not violations.empty:
                    errors_list.append(violations)

            if errors_list:
                errors = pr.concat(errors_list, ignore_index=True)

                # Format error output
                errors["thr_lower_str"] = (
                    errors["thr_lower"].round(4).astype(str) if "thr_lower" in errors.columns else "N/A"
                )
                errors["thr_upper_str"] = errors["thr_upper"].apply(lambda x: f"{x:.4f}" if pd.notnull(x) else "N/A")

                errors["error_desc"] = (
                    "Decile "
                    + errors["decile"].astype(str)
                    + ": avg="
                    + errors["avg"].round(4).astype(str)
                    + ", thr_lower="
                    + errors["thr_lower_str"]
                    + ", thr_upper="
                    + errors["thr_upper_str"]
                )

                tb_error = errors[["country", "year", "error_desc"]].copy()
                paths.log.fatal(
                    f"""{len(tb_error)} observations for avg {welfare_type} ({equivalence_scale}) are not between the corresponding thresholds:
                    {_tabulate(tb_error)}"""
                )


def _tabulate(tb: Table, headers="keys", tablefmt=TABLEFMT, **kwargs):
    """Helper function to format tables for display"""
    if LONG_FORMAT:
        return tabulate(tb, headers=headers, tablefmt=tablefmt, **kwargs)
    else:
        return tabulate(tb.head(5), headers=headers, tablefmt=tablefmt, **kwargs)


def check_poverty_range(tb_poverty: Table) -> None:
    """
    Check that headcount_ratio is between 0 and 100
    """
    tb = tb_poverty.reset_index()

    # Check headcount_ratio values for all combinations
    for welfare_type in tb["welfare_type"].unique():
        for equivalence_scale in tb["equivalence_scale"].unique():
            mask = (tb["welfare_type"] == welfare_type) & (tb["equivalence_scale"] == equivalence_scale)
            tb_subset = tb[mask].copy()

            # Check headcount_ratio
            if "headcount_ratio" in tb_subset.columns:
                ratio_mask = (tb_subset["headcount_ratio"] > 100) | (tb_subset["headcount_ratio"] < 0)
                any_error = ratio_mask.any()

                if any_error:
                    tb_error = tb_subset[ratio_mask][["country", "year", "poverty_line", "headcount_ratio"]].copy()
                    paths.log.fatal(
                        f"""{len(tb_error)} headcount_ratio values for {welfare_type} ({equivalence_scale}) are not between 0 and 100:
                        {_tabulate(tb_error)}"""
                    )


def check_poverty_monotonicity(tb_poverty: Table) -> None:
    """
    Check monotonicity of headcount_ratio and headcount across poverty lines.
    Uses dynamic sorting: numeric for absolute poverty, alphabetic for relative poverty.
    """
    tb = tb_poverty.reset_index()

    # Identify absolute vs relative poverty by checking if poverty_line is numeric or contains text
    # Absolute poverty lines are numeric strings like "3", "4.20", "8.30"
    # Relative poverty lines contain text like "40% of the median"
    tb["is_absolute"] = tb["poverty_line"].str.contains("%", na=False) == False

    # Split into absolute and relative
    tb_absolute = tb[tb["is_absolute"]].copy()
    tb_relative = tb[~tb["is_absolute"]].copy()

    # Check absolute poverty with numeric sorting
    if not tb_absolute.empty:
        _check_poverty_monotonicity_by_type(
            tb_absolute, poverty_type="absolute", metrics=["headcount_ratio", "headcount"]
        )

    # Check relative poverty with alphabetic sorting
    if not tb_relative.empty:
        _check_poverty_monotonicity_by_type(
            tb_relative, poverty_type="relative", metrics=["headcount_ratio", "headcount"]
        )


def _check_poverty_monotonicity_by_type(tb: Table, poverty_type: str, metrics: list) -> None:
    """
    Check monotonicity for a specific poverty type (absolute or relative).

    Args:
        tb: Table with poverty data (already filtered to one type)
        poverty_type: "absolute" or "relative"
        metrics: List of metrics to check (e.g., ["headcount_ratio", "headcount"])
    """
    for welfare_type in tb["welfare_type"].unique():
        for equivalence_scale in tb["equivalence_scale"].unique():
            mask = (tb["welfare_type"] == welfare_type) & (tb["equivalence_scale"] == equivalence_scale)
            tb_subset = tb[mask].copy()

            if tb_subset.empty:
                continue

            # Apply sorting based on poverty type
            if poverty_type == "absolute":
                # Convert poverty_line to numeric and sort
                tb_subset["poverty_line_numeric"] = tb_subset["poverty_line"].astype(float)
                tb_subset = tb_subset.sort_values(["country", "year", "poverty_line_numeric"])
            else:  # relative
                # Sort alphabetically by poverty_line text
                tb_subset = tb_subset.sort_values(["country", "year", "poverty_line"])

            # Get unique poverty lines in sorted order
            if poverty_type == "absolute":
                poverty_lines_sorted = sorted(tb_subset["poverty_line"].unique(), key=float)
            else:
                poverty_lines_sorted = sorted(tb_subset["poverty_line"].unique())

            # Require at least 2 poverty lines to check monotonicity
            if len(poverty_lines_sorted) < 2:
                continue

            # Check each metric
            for metric in metrics:
                if metric not in tb_subset.columns:
                    continue

                # Work with a fresh copy for each metric
                tb_metric = tb_subset.copy()

                # First, ensure each country-year has at least 2 poverty lines (minimum for monotonicity check)
                poverty_line_counts = (
                    tb_metric.groupby(["country", "year"])["poverty_line"].nunique().reset_index(name="line_count")
                )
                tb_metric = pr.merge(tb_metric, poverty_line_counts, on=["country", "year"])
                tb_metric = tb_metric[tb_metric["line_count"] >= 2].copy()

                if tb_metric.empty:
                    continue

                # Filter out country-years with any NULL values in the metric
                null_counts = (
                    tb_metric.groupby(["country", "year"])[metric]
                    .apply(lambda x: x.isnull().sum())
                    .reset_index(name="null_count")
                )
                tb_complete = pr.merge(tb_metric, null_counts, on=["country", "year"])
                tb_complete = tb_complete[tb_complete["null_count"] == 0].copy()

                if tb_complete.empty:
                    continue

                # Use shift() to compare consecutive values
                tb_complete["prev_value"] = tb_complete.groupby(["country", "year"])[metric].shift(1)
                tb_complete["prev_poverty_line"] = tb_complete.groupby(["country", "year"])["poverty_line"].shift(1)

                # Check monotonicity: current value should be >= previous value
                tb_complete["is_monotonic"] = (tb_complete[metric] >= tb_complete["prev_value"]) | tb_complete[
                    "prev_value"
                ].isnull()

                # Find violations
                errors = tb_complete[~tb_complete["is_monotonic"] & tb_complete["prev_value"].notnull()].copy()

                if not errors.empty:
                    # Format error output with 4 decimal precision
                    errors["error_desc"] = (
                        errors["prev_poverty_line"]
                        + " ("
                        + errors["prev_value"].round(4).astype(str)
                        + ") -> "
                        + errors["poverty_line"]
                        + " ("
                        + errors[metric].round(4).astype(str)
                        + ")"
                    )

                    tb_error = errors[["country", "year", "error_desc"]].copy()
                    paths.log.fatal(
                        f"""{len(tb_error)} observations for {metric} ({poverty_type} poverty, {welfare_type}, {equivalence_scale}) are not monotonically increasing:
                        {_tabulate(tb_error)}"""
                    )

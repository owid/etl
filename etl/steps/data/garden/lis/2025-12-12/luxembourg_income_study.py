"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

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

    # Add decile column with NaN to mean_median table (will be handled by pandas during concat)
    # Don't add the column - let concat handle it

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

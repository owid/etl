"""
Load a meadow dataset and create a garden dataset.

When running this step in an update, be sure to check all the outputs and logs to ensure the data is correct.

NOTE: To extract the log of the process (to review sanity checks, for example), run the following command in the terminal (and set DEBUG = True in the code):
    nohup uv run etl run world_bank_pip > output_pip.log 2>&1 &

"""

from typing import List, Tuple

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from structlog import get_logger
from tabulate import tabulate

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()

# Define absolute poverty lines used depending on PPP version
# NOTE: Modify if poverty lines are updated from source
POVLINES_DICT = {
    2017: ["100", "215", "365", "500", "685", "700", "1000", "2000", "3000", "4000"],
    2021: ["100", "300", "420", "500", "700", "830", "1000", "2000", "3000", "4000"],
}

# Define international poverty lines as the second value in each list in POVLINES_DICT
INTERNATIONAL_POVERTY_LINES = {ppp_year: poverty_lines[1] for ppp_year, poverty_lines in POVLINES_DICT.items()}

# Set precision of sanity checks for percentages
PRECISION_PERCENTAGE = 0.1

# Define regions in the dataset
REGIONS_LIST = [
    "East Asia and Pacific (PIP)",
    "Eastern and Southern Africa (PIP)",
    "Europe and Central Asia (PIP)",
    "Latin America and the Caribbean (PIP)",
    "Middle East and North Africa (PIP)",
    "Other high income countries (PIP)",
    "South Asia (PIP)",
    "Sub-Saharan Africa (PIP)",
    "Western and Central Africa (PIP)",
    "World",
    "World (excluding China)",
    "World (excluding India)",
]

# Define countries expected to have both income and consumption data
COUNTRIES_WITH_INCOME_AND_CONSUMPTION = [
    "Albania",
    "Armenia",
    "Belarus",
    "Belize",
    "Bulgaria",
    "China",
    "China (rural)",
    "China (urban)",
    "Croatia",
    "Estonia",
    "Haiti",
    "Hungary",
    "Kazakhstan",
    "Kosovo",
    "Kyrgyzstan",
    "Latvia",
    "Lithuania",
    "Montenegro",
    "Namibia",
    "Nepal",
    "Nicaragua",
    "North Macedonia",
    "Peru",
    "Philippines",
    "Poland",
    "Romania",
    "Russia",
    "Saint Lucia",
    "Serbia",
    "Seychelles",
    "Slovakia",
    "Slovenia",
    "Turkey",
    "Ukraine",
    "Uzbekistan",
]

# Set debug mode
DEBUG = False

# Set table format when printing
TABLEFMT = "pretty"

# Define indicators that don't depend on poverty lines
INDICATORS_NOT_DEPENDENT_ON_POVLINES_NOR_DECILES = [
    "mean",
    "median",
    "mld",
    "gini",
    "polarization",
    "cpi",
    "ppp",
    "reporting_pop",
    "reporting_gdp",
    "reporting_pce",
    "spl",
    "spr",
    "pg",
    "estimate_type",
    "pop_in_poverty",
    "bottom50_share",
    "middle40_share",
    "palma_ratio",
    "s80_s20_ratio",
    "p90_p10_ratio",
    "p90_p50_ratio",
    "p50_p10_ratio",
    "top1_thr",
    "top1_share",
    "top1_avg",
    "top90_99_share",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("world_bank_pip")

    # Read tables from meadow dataset.
    # Key indicators
    tb = ds_meadow.read("world_bank_pip")

    # Percentiles
    tb_percentiles = ds_meadow.read("world_bank_pip_percentiles")

    # Region definitions
    tb_region_definitions = ds_meadow.read("world_bank_pip_regions")

    #
    # Process data.
    #
    tb = create_new_indicators_and_format(tb=tb)
    tb = calculate_inequality_indicators(tb=tb)

    # Harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path, warn_on_unused_countries=False)
    tb_percentiles = geo.harmonize_countries(
        df=tb_percentiles, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    tb_region_definitions = harmonize_region_name(tb=tb_region_definitions)

    # Make share a percentage in tb_percentiles
    tb_percentiles["share"] *= 100

    # Add top 1 percentile to the table
    tb = add_top_1_percentile(tb=tb, tb_percentiles=tb_percentiles)

    # Show regional data from 1990 onwards
    tb = regional_data_from_1990(tb=tb, regions_list=REGIONS_LIST)
    tb_percentiles = regional_data_from_1990(tb=tb_percentiles, regions_list=REGIONS_LIST)

    # Amend the entity to reflect if data refers to urban or rural only
    tb = identify_rural_urban(tb=tb)

    # Create stacked variables from headcount and headcount_ratio
    tb = create_stacked_variables(tb=tb)

    # NOTE: In the future, I could modify it to handle issues between PPP versions better
    # Currently, for some checks, if there is an issue for the old PPP version, it will be deleted for the current one too (it is not a big deal)
    tb = sanity_checks(tb=tb)

    tb = make_distributional_indicators_long(tb=tb)

    tb = make_relative_poverty_long(tb=tb)

    tb = make_poverty_line_null_for_non_dimensional_indicators(tb=tb)

    # Drop ppp years for CPI
    tb = make_cpi_not_depending_on_ppp(tb=tb)

    # Separate out consumption-only, income-only. Also, create a table with both income and consumption
    tb, tb_inc_or_cons_complete, tb_inc_or_cons_smooth = inc_or_cons_data(tb=tb)

    # Create survey count dataset, by counting the number of surveys available for each country in the past decade
    tb_inc_or_cons_smooth = survey_count(tb=tb_inc_or_cons_smooth)

    # Add region definitions
    tb_inc_or_cons_smooth = add_region_definitions(
        tb=tb_inc_or_cons_smooth, tb_region_definitions=tb_region_definitions
    )

    # Concatenate the final table
    tb = pr.concat([tb, tb_inc_or_cons_smooth], ignore_index=True)

    # Drop columns not needed
    tb = drop_columns(tb)
    tb_inc_or_cons_complete = drop_columns(tb_inc_or_cons_complete)

    # Make empty values of survey_comparability to "No spells"
    tb["survey_comparability"] = tb["survey_comparability"].astype(str)
    tb["survey_comparability"] = tb["survey_comparability"].replace("<NA>", "No spells")

    # Do the same for poverty_line
    tb["poverty_line"] = tb["poverty_line"].fillna("No poverty line")

    # Improve table format.
    tb = tb.format(
        ["country", "year", "ppp_version", "poverty_line", "welfare_type", "decile", "table", "survey_comparability"],
    )
    tb_inc_or_cons_complete = tb_inc_or_cons_complete.format(
        ["country", "year", "ppp_version", "poverty_line", "welfare_type", "decile"],
        short_name="full_dataset_without_smoothing",
    )
    tb_percentiles = tb_percentiles.format(
        ["country", "year", "ppp_version", "welfare_type", "reporting_level", "percentile"],
    )

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[tb, tb_inc_or_cons_complete, tb_percentiles],
        default_metadata=ds_meadow.metadata,
    )

    # Save garden dataset.
    ds_garden.save()


def create_new_indicators_and_format(tb: Table) -> Table:
    """
    Create new indicators from the existing ones and format names and shares
    """
    # rename columns
    tb = tb.rename(columns={"headcount": "headcount_ratio", "poverty_gap": "poverty_gap_index"}, errors="raise")

    # Drop columns not needed
    tb = tb.drop(
        columns=[
            "country_code",
            "survey_acronym",
            "survey_coverage",
            "survey_year",
            "comparable_spell",
            "distribution_type",
            "estimation_type",
        ],
        errors="raise",
    )

    # Changing the decile(i) variables for decile(i)_share
    for i in range(1, 11):
        tb = tb.rename(columns={f"decile{i}": f"decile{i}_share"}, errors="raise")

    # Calculate number in poverty
    tb["headcount"] = tb["headcount_ratio"] * tb["reporting_pop"]
    tb["headcount"] = tb["headcount"].round(0)

    # Calculate shortfall of incomes
    tb["total_shortfall"] = tb["poverty_gap_index"] * tb["poverty_line"] * tb["reporting_pop"]

    # Calculate average shortfall of incomes (averaged across population in poverty)
    tb["avg_shortfall"] = tb["total_shortfall"] / tb["headcount"]

    # Calculate income gap ratio (according to Ravallion's definition)
    tb["income_gap_ratio"] = (tb["total_shortfall"] / tb["headcount"]) / tb["poverty_line"]

    # Make total_shortfall by year
    tb["total_shortfall"] *= 365

    # Same for relative poverty
    for pct in [40, 50, 60]:
        tb[f"headcount_{pct}_median"] = tb[f"headcount_ratio_{pct}_median"] * tb["reporting_pop"]
        tb[f"headcount_{pct}_median"] = tb[f"headcount_{pct}_median"].round(0)
        tb[f"total_shortfall_{pct}_median"] = (
            tb[f"poverty_gap_index_{pct}_median"] * tb["median"] * pct / 100 * tb["reporting_pop"]
        )
        tb[f"avg_shortfall_{pct}_median"] = tb[f"total_shortfall_{pct}_median"] / tb[f"headcount_{pct}_median"]
        tb[f"income_gap_ratio_{pct}_median"] = (tb[f"total_shortfall_{pct}_median"] / tb[f"headcount_{pct}_median"]) / (
            tb["median"] * pct / 100
        )
        # Make total_shortfall by year
        tb[f"total_shortfall_{pct}_median"] *= 365

    # Shares to percentages
    # executing the function over list of vars
    pct_indicators = [
        "headcount_ratio",
        "income_gap_ratio",
        "poverty_gap_index",
        "headcount_ratio_40_median",
        "headcount_ratio_50_median",
        "headcount_ratio_60_median",
        "income_gap_ratio_40_median",
        "income_gap_ratio_50_median",
        "income_gap_ratio_60_median",
        "poverty_gap_index_40_median",
        "poverty_gap_index_50_median",
        "poverty_gap_index_60_median",
    ]
    tb.loc[:, pct_indicators] = tb[pct_indicators] * 100

    # Make the poverty lines in cents for easier handling
    tb["poverty_line"] = round(tb["poverty_line"] * 100).astype(int).astype(str)

    # Round reporting_pop to int
    tb["reporting_pop"] = tb["reporting_pop"].round(0).astype(int)

    return tb


def calculate_inequality_indicators(tb: Table) -> Table:
    """
    Calculate inequality indicators: decile averages and ratios
    """

    col_decile_share = []
    col_decile_avg = []
    col_decile_thr = []

    for i in range(1, 11):
        # Because there are only 9 thresholds
        if i != 10:
            varname_thr = f"decile{i}_thr"
            col_decile_thr.append(varname_thr)

        # Define the share and average variables
        varname_share = f"decile{i}_share"
        varname_avg = f"decile{i}_avg"

        # Calculate the averages from the shares data
        tb[varname_avg] = tb[varname_share] * tb["mean"] / 0.1

        # Save the variable names to the lists
        col_decile_share.append(varname_share)
        col_decile_avg.append(varname_avg)

    # Multiplies decile columns by 100
    tb.loc[:, col_decile_share] = tb[col_decile_share] * 100

    # Create bottom 50 and middle 40% shares
    tb["bottom50_share"] = (
        tb["decile1_share"] + tb["decile2_share"] + tb["decile3_share"] + tb["decile4_share"] + tb["decile5_share"]
    )
    tb["middle40_share"] = tb["decile6_share"] + tb["decile7_share"] + tb["decile8_share"] + tb["decile9_share"]

    # Palma ratio and other average/share ratios
    tb["palma_ratio"] = tb["decile10_share"] / (
        tb["decile1_share"] + tb["decile2_share"] + tb["decile3_share"] + tb["decile4_share"]
    )
    tb["s80_s20_ratio"] = (tb["decile9_share"] + tb["decile10_share"]) / (tb["decile1_share"] + tb["decile2_share"])
    tb["p90_p10_ratio"] = tb["decile9_thr"] / tb["decile1_thr"]
    tb["p90_p50_ratio"] = tb["decile9_thr"] / tb["decile5_thr"]
    tb["p50_p10_ratio"] = tb["decile5_thr"] / tb["decile1_thr"]

    # Replace infinite values with nulls
    tb = tb.replace([np.inf, -np.inf], pd.NA)

    return tb


def harmonize_region_name(tb: Table) -> Table:
    """
    Harmonize country and region_name in tb_region_definitions, using the harmonizing tool, but removing the (PIP) suffix
    """

    tb = tb.copy()

    for country_col in ["country", "region_name"]:
        tb = geo.harmonize_countries(
            df=tb, country_col=country_col, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
        )

    # Remove (PIP) from region_name
    tb["region_name"] = tb["region_name"].str.replace(r" \(PIP\)", "", regex=True)

    return tb


def add_top_1_percentile(tb: Table, tb_percentiles: Table) -> Table:
    """
    Add top 1% data (share, average, threshold) to the main indicators
    Also, calculate the share of the top 90-99%
    """

    tb = tb.copy()
    tb_percentiles = tb_percentiles.copy()

    # Create different tables for thresholds and shares/averages
    tb_percentiles_thr = tb_percentiles[tb_percentiles["percentile"] == 99].copy()
    tb_percentiles_share = tb_percentiles[tb_percentiles["percentile"] == 100].copy()

    # Select appropriate columns and rename
    tb_percentiles_thr = tb_percentiles_thr[
        ["ppp_version", "country", "year", "reporting_level", "welfare_type", "thr"]
    ]
    tb_percentiles_thr = tb_percentiles_thr.rename(columns={"thr": "top1_thr"}, errors="raise")

    tb_percentiles_share = tb_percentiles_share[
        ["ppp_version", "country", "year", "reporting_level", "welfare_type", "share", "avg"]
    ]
    tb_percentiles_share = tb_percentiles_share.rename(
        columns={"share": "top1_share", "avg": "top1_avg"}, errors="raise"
    )

    tb_filled, tb_unfilled = separate_filled_and_unfilled_data(tb=tb)

    # Merge with the main table
    tb_unfilled = pr.merge(
        tb_unfilled,
        tb_percentiles_thr,
        on=["ppp_version", "country", "year", "reporting_level", "welfare_type"],
        how="left",
    )
    tb_unfilled = pr.merge(
        tb_unfilled,
        tb_percentiles_share,
        on=["ppp_version", "country", "year", "reporting_level", "welfare_type"],
        how="left",
    )

    # Now I can calculate the share of the top 90-99%
    tb_unfilled["top90_99_share"] = tb_unfilled["decile10_share"] - tb_unfilled["top1_share"]

    # Concatenate the filled and unfilled tables
    tb = pr.concat([tb_filled, tb_unfilled], ignore_index=True)

    return tb


def regional_data_from_1990(tb: Table, regions_list: list) -> Table:
    """
    Select regional data only from 1990 onwards, due to the uncertainty in 1980s data
    """
    # Create a regions table
    tb_regions = tb[(tb["year"] >= 1990) & (tb["country"].isin(regions_list))].reset_index(drop=True)

    # Remove regions from tb
    tb = tb[~tb["country"].isin(regions_list)].reset_index(drop=True)

    # Concatenate both tables
    tb = pr.concat([tb, tb_regions], ignore_index=True)

    return tb


def identify_rural_urban(tb: Table) -> Table:
    """
    Amend the entity to reflect if data refers to urban or rural only
    """

    # Make country and reporting_level columns into strings
    tb["country"] = tb["country"].astype(str)
    tb["reporting_level"] = tb["reporting_level"].astype(str)

    # Define condition to identify urban and rural data
    condition_urban_rural = tb["reporting_level"].isin(["urban", "rural"])

    # Change the country name only for urban and rural data
    tb.loc[(condition_urban_rural), "country"] = (
        tb.loc[(condition_urban_rural), "country"] + " (" + tb.loc[(condition_urban_rural), "reporting_level"] + ")"
    )

    # Drop reporting_level column
    tb = tb.drop(columns=["reporting_level"], errors="raise")

    return tb


def create_stacked_variables(tb: Table) -> Tuple[Table, list, list]:
    """
    Create stacked variables from the indicators to plot them as stacked area/bar charts
    """

    tb = tb.copy()

    # Define headcount_above and headcount_ratio_above variables
    tb["headcount_above"] = tb["reporting_pop"] - tb["headcount"]
    tb["headcount_ratio_above"] = tb["headcount_above"] / tb["reporting_pop"]

    # Make headcount_ratio_above a percentage
    tb["headcount_ratio_above"] = tb["headcount_ratio_above"] * 100

    # Define stacked variables as headcount and headcount_ratio between poverty lines
    # Select only the necessary columns
    tb_pivot = tb[
        [
            "country",
            "year",
            "welfare_type",
            "filled",
            "ppp_version",
            "poverty_line",
            "headcount_ratio",
            "headcount",
            "reporting_pop",
        ]
    ].copy()

    # Pivot
    tb_pivot = pivot_table(
        tb=tb_pivot,
        index=["country", "year", "welfare_type", "filled"],
        columns=["ppp_version", "poverty_line"],
    )

    for ppp_year, povlines in POVLINES_DICT.items():
        # Remove "500" and "700" from povlines, since I don't want to use them for this indicator
        povlines = [p for p in povlines if p not in ["500", "700"]]

        for i in range(len(povlines)):
            # if it's the first value only continue
            if i == 0:
                continue

            # If it's the last value calculate the people between this value and the previous
            # and also the people over this poverty line (and percentages)
            else:
                varname_n = ("headcount_between", ppp_year, f"{povlines[i-1]} and {povlines[i]}")
                varname_pct = ("headcount_ratio_between", ppp_year, f"{povlines[i-1]} and {povlines[i]}")
                tb_pivot[varname_n] = (
                    tb_pivot[("headcount", ppp_year, povlines[i])] - tb_pivot[("headcount", ppp_year, povlines[i - 1])]
                )
                tb_pivot[varname_pct] = tb_pivot[varname_n] / tb_pivot[("reporting_pop", ppp_year, povlines[i])]

                # Multiply by 100 to get percentage
                tb_pivot[varname_pct] = tb_pivot[varname_pct] * 100

        # Calculate stacked variables which "jump" the original order
        tb_pivot[("headcount_between", ppp_year, f"{povlines[1]} and {povlines[4]}")] = (
            tb_pivot[("headcount", ppp_year, povlines[4])] - tb_pivot[("headcount", ppp_year, povlines[1])]
        )
        tb_pivot[("headcount_between", ppp_year, f"{povlines[4]} and {povlines[6]}")] = (
            tb_pivot[("headcount", ppp_year, povlines[6])] - tb_pivot[("headcount", ppp_year, povlines[4])]
        )

        tb_pivot[("headcount_ratio_between", ppp_year, f"{povlines[1]} and {povlines[4]}")] = (
            tb_pivot[("headcount_ratio", ppp_year, povlines[4])] - tb_pivot[("headcount_ratio", ppp_year, povlines[1])]
        )
        tb_pivot[("headcount_ratio_between", ppp_year, f"{povlines[4]} and {povlines[6]}")] = (
            tb_pivot[("headcount_ratio", ppp_year, povlines[6])] - tb_pivot[("headcount_ratio", ppp_year, povlines[4])]
        )

        # Calculate stacked for indicators with 500 and 700 poverty lines
        tb_pivot[("headcount_between", ppp_year, f"{povlines[1]} and 500")] = (
            tb_pivot[("headcount", ppp_year, "500")] - tb_pivot[("headcount", ppp_year, povlines[1])]
        )
        tb_pivot[("headcount_between", ppp_year, "500 and 700")] = (
            tb_pivot[("headcount", ppp_year, "700")] - tb_pivot[("headcount", ppp_year, "500")]
        )
        tb_pivot[("headcount_between", ppp_year, "700 and 1000")] = (
            tb_pivot[("headcount", ppp_year, "1000")] - tb_pivot[("headcount", ppp_year, "700")]
        )

        tb_pivot[("headcount_ratio_between", ppp_year, f"{povlines[1]} and 500")] = (
            tb_pivot[("headcount_ratio", ppp_year, "500")] - tb_pivot[("headcount_ratio", ppp_year, povlines[1])]
        )
        tb_pivot[("headcount_ratio_between", ppp_year, "500 and 700")] = (
            tb_pivot[("headcount_ratio", ppp_year, "700")] - tb_pivot[("headcount_ratio", ppp_year, "500")]
        )
        tb_pivot[("headcount_ratio_between", ppp_year, "700 and 1000")] = (
            tb_pivot[("headcount_ratio", ppp_year, "1000")] - tb_pivot[("headcount_ratio", ppp_year, "700")]
        )

    # Now, only keep headcount_between and headcount_ratio_between, and headcount_above and headcount_ratio_above
    tb_pivot = tb_pivot.loc[
        :,
        tb_pivot.columns.get_level_values(0).isin(
            [
                "country",
                "year",
                "welfare_type",
                "headcount_between",
                "headcount_ratio_between",
            ]
        ),
    ]

    # Stack table
    tb_pivot = unpivot_table(
        tb=tb_pivot.reset_index(),
        index=["country", "year", "welfare_type", "filled"],
        level=["ppp_version", "poverty_line"],
    )

    # Merge with tb
    tb = pr.merge(
        tb,
        tb_pivot,
        on=["country", "year", "welfare_type", "filled", "poverty_line", "ppp_version"],
        how="outer",
    )

    # Copy metadata to recover origin
    tb["headcount_between"] = tb["headcount_between"].copy_metadata(tb["headcount"])
    tb["headcount_ratio_between"] = tb["headcount_ratio_between"].copy_metadata(tb["headcount_ratio"])

    return tb


def pivot_table(tb: Table, index: List[str], columns: List[str], join_column_levels_with: str | None = None) -> Table:
    """
    Pivot the table to calculate indicators more easily
    """

    tb_pivot = tb.pivot(
        index=index, columns=columns, fill_dimensions=False, join_column_levels_with=join_column_levels_with
    )

    return tb_pivot


def unpivot_table(tb: Table, index: List[str], level: List[str]) -> Table:
    """
    Unpivot table, using set_index and stack
    """
    tb = (
        tb.set_index(index)  # Set the desired index, including the additional columns
        .stack(level=level, future_stack=True)  # Stack the MultiIndex columns
        .reset_index()  # Reset the index to flatten the table
    )

    return tb


def sanity_checks(
    tb: Table,
) -> Table:
    """
    Sanity checks for the table
    """

    # Define index for pivot
    index = ["country", "year", "welfare_type", "filled"]

    # Pivot
    tb_pivot = pivot_table(tb=tb, index=index, columns=["ppp_version", "poverty_line"])

    # Reset index in tb_pivot
    tb_pivot = tb_pivot.reset_index()

    # Create lists of variables to check that depend on the poverty lines
    columns_poverty_lines = [
        "headcount",
        "headcount_ratio",
        "headcount_above",
        "headcount_ratio_above",
        "poverty_gap_index",
        "total_shortfall",
        # "watts", # not using them for analysis, since we don't plot them and they have missing values
        # "poverty_severity",
    ]

    # Create list for decile variables
    col_decile_share = []
    col_decile_thr = []
    col_decile_avg = []

    for i in range(1, 11):
        col_decile_share.append(f"decile{i}_share")
        col_decile_avg.append(f"decile{i}_avg")
        if i != 10:
            col_decile_thr.append(f"decile{i}_thr")

    for ppp_year, povlines in POVLINES_DICT.items():
        if DEBUG:
            log.info(f"SANITY CHECKS FOR {ppp_year} PRICES")
        # Save the number of observations before the checks
        obs_before_checks = len(tb_pivot)
        ############################
        # Negative values
        # Create a mask to check for negative values in columns_poverty_lines + col_decile_share + col_decile_avg + col_decile_thr
        cols_to_check = [
            (col, ppp_year, povline)
            for col in columns_poverty_lines
            + col_decile_share
            + col_decile_avg
            + col_decile_thr
            + ["mean", "median", "mld", "gini", "polarization"]
            for povline in povlines
        ]
        mask = tb_pivot.loc[:, cols_to_check].lt(0).any(axis=1)
        tb_error = tb_pivot[mask].reset_index(drop=True)

        if not tb_error.empty:
            log.fatal(
                f"""There are {len(tb_error)} observations with negative values! In:
                {tabulate(tb_error[index], headers = 'keys', tablefmt = TABLEFMT)}"""
            )
            # NOTE: Check if we want to delete these observations
            # tb_pivot = tb_pivot[~mask].reset_index(drop=True)

        ############################
        # stacked values not adding up to 100%
        # Define stacked columns
        col_stacked_pct_dict = {}
        col_stacked_n_dict = {}
        # Initialize the stacked column lists representing all the possible intervals
        col_stacked_pct_all = []
        col_stacked_n_all = []

        # Remove "500" and "700" from povlines, since I don't want to use them for stacked indicators
        povlines_stacked = [p for p in povlines if p not in ["500", "700"]]
        for i in range(len(povlines_stacked)):
            # if it's the first value only continue
            if i == 0:
                continue

            # If it's the last value calculate the people between this value and the previous
            # and also the people over this poverty line (and percentages)
            else:
                varname_n = ("headcount_between", ppp_year, f"{povlines_stacked[i-1]} and {povlines_stacked[i]}")
                varname_pct = (
                    "headcount_ratio_between",
                    ppp_year,
                    f"{povlines_stacked[i-1]} and {povlines_stacked[i]}",
                )
                col_stacked_n_all.append(varname_n)
                col_stacked_pct_all.append(varname_pct)

        col_stacked_pct_all = (
            [("headcount_ratio", ppp_year, povlines_stacked[0])]
            + col_stacked_pct_all
            + [("headcount_ratio_above", ppp_year, povlines_stacked[-1])]
        )

        col_stacked_n_all = (
            [("headcount", ppp_year, povlines_stacked[0])]
            + col_stacked_n_all
            + [("headcount_above", ppp_year, povlines_stacked[-1])]
        )

        col_stacked_pct_dict[ppp_year] = {"all": col_stacked_pct_all}
        col_stacked_n_dict[ppp_year] = {"all": col_stacked_n_all}

        # Define the stacked columns for a reduced set of intervals
        col_stacked_pct_reduced = [
            ("headcount_ratio", ppp_year, povlines_stacked[1]),
            ("headcount_ratio_between", ppp_year, f"{povlines_stacked[1]} and {povlines_stacked[4]}"),
            ("headcount_ratio_between", ppp_year, f"{povlines_stacked[4]} and {povlines_stacked[6]}"),
            ("headcount_ratio_above", ppp_year, povlines_stacked[6]),
        ]
        col_stacked_n_reduced = [
            ("headcount", ppp_year, povlines_stacked[1]),
            ("headcount_between", ppp_year, f"{povlines_stacked[1]} and {povlines_stacked[4]}"),
            ("headcount_between", ppp_year, f"{povlines_stacked[4]} and {povlines_stacked[6]}"),
            ("headcount_above", ppp_year, povlines_stacked[6]),
        ]

        # Add the reduced columns to the dictionary
        col_stacked_pct_dict[ppp_year]["reduced"] = col_stacked_pct_reduced
        col_stacked_n_dict[ppp_year]["reduced"] = col_stacked_n_reduced

        # Calculate and check the sum of the stacked values
        tb_pivot["sum_pct"] = tb_pivot[col_stacked_pct_dict[ppp_year]["all"]].sum(axis=1)
        mask = (tb_pivot["sum_pct"] >= 100 + PRECISION_PERCENTAGE) | (tb_pivot["sum_pct"] <= 100 - PRECISION_PERCENTAGE)
        tb_error = tb_pivot[mask].reset_index(drop=True)

        if not tb_error.empty:
            if DEBUG:
                log.warning(
                    f"""{len(tb_error)} observations of all stacked values are not adding up to 100% and will be deleted:
                    {tabulate(tb_error[index + ['sum_pct']], headers = 'keys', tablefmt = TABLEFMT, floatfmt=".1f")}"""
                )
            tb_pivot = tb_pivot[~mask].reset_index(drop=True)

        # For the reduced set of intervals
        tb_pivot["sum_pct"] = tb_pivot[col_stacked_pct_dict[ppp_year]["reduced"]].sum(axis=1)
        mask = (tb_pivot["sum_pct"] >= 100 + PRECISION_PERCENTAGE) | (tb_pivot["sum_pct"] <= 100 - PRECISION_PERCENTAGE)
        tb_error = tb_pivot[mask].reset_index(drop=True)

        if not tb_error.empty:
            if DEBUG:
                log.warning(
                    f"""{len(tb_error)} observations of the reduced set of stacked values are not adding up to 100% and will be deleted:
                    {tabulate(tb_error[index + ['sum_pct']], headers = 'keys', tablefmt = TABLEFMT, floatfmt=".1f")}"""
                )
            tb_pivot = tb_pivot[~mask].reset_index(drop=True)

        ############################
        # missing poverty values (headcount, poverty gap, total shortfall)
        cols_to_check = [(col, ppp_year, povline) for col in columns_poverty_lines for povline in povlines]
        mask = (tb_pivot.loc[:, cols_to_check].isna().any(axis=1)) & (
            ~tb_pivot["country"].isin(["World (excluding China)", "World (excluding India)"])
        )
        tb_error = tb_pivot[mask].reset_index(drop=True)

        if not tb_error.empty:
            if DEBUG:
                log.warning(
                    f"""There are {len(tb_error)} observations with missing poverty values and will be deleted:
                    {tabulate(tb_error[(index + ["headcount_ratio"])], headers = 'keys', tablefmt = TABLEFMT)}"""
                )

            tb_pivot = tb_pivot[~mask].reset_index(drop=True)

        ############################
        # headcount monotonicity check
        # Define the headcount columns for the current ppp_year
        col_headcount = [("headcount", ppp_year, povline) for povline in povlines]
        m_check_vars = []
        for i in range(len(col_headcount)):
            if i > 0:
                check_varname = f"m_check_{i}"
                tb_pivot[check_varname] = tb_pivot[col_headcount[i]] >= tb_pivot[col_headcount[i - 1]]
                m_check_vars.append(check_varname)
        tb_pivot["check_total"] = tb_pivot[m_check_vars].all(axis=1)

        tb_error = tb_pivot[~tb_pivot["check_total"]].reset_index(drop=True)

        if not tb_error.empty:
            if DEBUG:
                log.warning(
                    f"""There are {len(tb_error)} observations with headcount not monotonically increasing and will be deleted:
                    {tabulate(tb_error[index], headers = 'keys', tablefmt = TABLEFMT, floatfmt="0.0f")}"""
                )
            tb_pivot = tb_pivot[tb_pivot["check_total"]].reset_index(drop=True)

        ############################
        # Threshold monotonicity check
        cols_to_check = [(col, ppp_year, povlines[1]) for col in col_decile_thr]
        m_check_vars = []
        for i in range(len(cols_to_check)):
            if i > 0:
                check_varname = f"m_check_{i}"
                tb_pivot[check_varname] = tb_pivot[cols_to_check[i]] >= tb_pivot[cols_to_check[i - 1]]
                m_check_vars.append(check_varname)

        tb_pivot["check_total"] = tb_pivot[m_check_vars].all(axis=1)

        # Drop rows if columns in col_decile_thr are all null. Keep if some are null
        mask = (~tb_pivot["check_total"]) & (tb_pivot[cols_to_check].notnull().any(axis=1))

        tb_error = tb_pivot[mask].reset_index(drop=True)

        if not tb_error.empty:
            if DEBUG:
                log.warning(
                    f"""There are {len(tb_error)} observations with thresholds not monotonically increasing and will be deleted:
                    {tabulate(tb_error[index], headers = 'keys', tablefmt = TABLEFMT)}"""
                )
            tb_pivot = tb_pivot[~mask].reset_index(drop=True)

        ############################
        # Shares monotonicity check
        cols_to_check = [(col, ppp_year, povlines[1]) for col in col_decile_share]
        m_check_vars = []
        for i in range(len(cols_to_check)):
            if i > 0:
                check_varname = f"m_check_{i}"
                tb_pivot[check_varname] = tb_pivot[cols_to_check[i]] >= tb_pivot[cols_to_check[i - 1]]
                m_check_vars.append(check_varname)

        tb_pivot["check_total"] = tb_pivot[m_check_vars].all(axis=1)

        # Drop rows if columns in col_decile_share are all null. Keep if some are null
        mask = (~tb_pivot["check_total"]) & (tb_pivot[cols_to_check].notnull().any(axis=1))
        tb_error = tb_pivot[mask].reset_index(drop=True)

        if not tb_error.empty:
            if DEBUG:
                log.warning(
                    f"""There are {len(tb_error)} observations with shares not monotonically increasing and will be deleted:
                    {tabulate(tb_error[index], headers = 'keys', tablefmt = TABLEFMT, floatfmt=".1f")}"""
                )
            tb_pivot = tb_pivot[~mask].reset_index(drop=True)

        ############################
        # Shares not adding up to 100%

        cols_to_check = [(col, ppp_year, povlines[1]) for col in col_decile_share]
        tb_pivot["sum_pct"] = tb_pivot[cols_to_check].sum(axis=1)

        # Drop rows if columns in col_decile_share are all null. Keep if some are null
        mask = (tb_pivot["sum_pct"] >= 100 + PRECISION_PERCENTAGE) | (
            tb_pivot["sum_pct"] <= 100 - PRECISION_PERCENTAGE
        ) & (tb_pivot[cols_to_check].notnull().any(axis=1))
        tb_error = tb_pivot[mask].reset_index(drop=True)

        if not tb_error.empty:
            if DEBUG:
                log.warning(
                    f"""{len(tb_error)} observations of shares are not adding up to 100% and will be deleted:
                    {tabulate(tb_error[index + ['sum_pct']], headers = 'keys', tablefmt = TABLEFMT, floatfmt=".1f")}"""
                )
            tb_pivot = tb_pivot[~mask].reset_index(drop=True)

        ############################
        # Shares not adding up to 100% (top 1%)

        # Define columns to add up to 100%
        col_decile_share_top = ["bottom50_share", "middle40_share", "top90_99_share", "top1_share"]
        cols_to_check = [(col, ppp_year, povlines[1]) for col in col_decile_share_top]
        tb_pivot["sum_pct"] = tb_pivot[cols_to_check].sum(axis=1)

        # Drop rows if columns in col_decile_share_top are all null. Keep if some are null
        mask = (tb_pivot["sum_pct"] >= 100 + PRECISION_PERCENTAGE) | (
            tb_pivot["sum_pct"] <= 100 - PRECISION_PERCENTAGE
        ) & (tb_pivot[cols_to_check].notnull().any(axis=1))
        tb_error = tb_pivot[mask].reset_index(drop=True)

        if not tb_error.empty:
            if DEBUG:
                log.warning(
                    f"""{len(tb_error)} observations of shares (with top 1%) are not adding up to 100% and will be converted to null:
                    {tabulate(tb_error[index + ['sum_pct']], headers = 'keys', tablefmt = TABLEFMT, floatfmt=".1f")}"""
                )
            # Make columns None if mask is True
            tb_pivot.loc[mask, [("top90_99_share", ppp_year, povlines[1]), ("top1_share", ppp_year, povlines[1])]] = (
                pd.NA
            )

        ############################
        # delete columns created for the checks
        tb_pivot = tb_pivot.drop(columns=m_check_vars + ["m_check_1", "check_total", "sum_pct"], errors="raise")

        obs_after_checks = len(tb_pivot)
        log.info(f"Sanity checks deleted {obs_before_checks - obs_after_checks} observations for {ppp_year} PPPs.")

    # Restore the format of the table
    tb = unpivot_table(tb=tb_pivot, index=index, level=["ppp_version", "poverty_line"])

    return tb


def inc_or_cons_data(tb: Table) -> Tuple[Table, Table]:
    """
    Separate income and consumption data
    """

    tb_filled, tb_unfilled = separate_filled_and_unfilled_data(tb=tb)

    # Make a copy of the table
    tb_spells = tb_unfilled.copy()
    tb_no_spells = tb_unfilled.copy()

    # Generate tb_inc_spells and tb_cons_spells
    tb_inc_spells = tb_spells[tb_spells["welfare_type"] == "income"].reset_index(drop=True)
    tb_cons_spells = tb_spells[tb_spells["welfare_type"] == "consumption"].reset_index(drop=True)

    # Drop the survey_comparability column for tb_no_spells
    tb_no_spells = tb_no_spells.drop(columns=["survey_comparability"], errors="raise")

    # Generate tb_inc_no_spells and tb_cons_no_spells
    tb_inc_no_spells = tb_no_spells[tb_no_spells["welfare_type"] == "income"].reset_index(drop=True)
    tb_cons_no_spells = tb_no_spells[tb_no_spells["welfare_type"] == "consumption"].reset_index(drop=True)

    # Create tb_no_spells_smooth, which cleans tb_no_spells, by removing jumps generated by changes in welfare_type
    tb_no_spells_smooth = create_smooth_inc_cons_series(tb_no_spells)

    check_jumps_in_grapher_dataset(tb_no_spells_smooth)

    # Add the column table, identifying the type of table to use in Grapher
    tb_inc_spells["table"] = "Income with spells"
    tb_cons_spells["table"] = "Consumption with spells"
    tb_inc_no_spells["table"] = "Income"
    tb_cons_no_spells["table"] = "Consumption"
    tb_no_spells_smooth["table"] = "Income or consumption consolidated"
    tb_filled["table"] = "Income or consumption intra/extrapolated"

    # Also, rename welfare_type to "Income or consumption" for tb_no_spells_smooth
    tb_no_spells_smooth["welfare_type"] = "income or consumption"
    tb_filled["welfare_type"] = "income or consumption"

    # Fill missing values in welfare_type for tb_no_spells
    tb_no_spells["welfare_type"] = tb_no_spells["welfare_type"].fillna("income or consumption")

    # Concatenate all these tables
    tb = pr.concat(
        [
            tb_inc_spells,
            tb_cons_spells,
            tb_inc_no_spells,
            tb_cons_no_spells,
            tb_filled,
        ],
        ignore_index=True,
    )

    # Remove filled column
    tb = tb.drop(columns=["filled"], errors="raise")
    tb_no_spells = tb_no_spells.drop(columns=["filled"], errors="raise")
    tb_no_spells_smooth = tb_no_spells_smooth.drop(columns=["filled"], errors="raise")

    return tb, tb_no_spells, tb_no_spells_smooth


def create_smooth_inc_cons_series(tb: Table) -> Table:
    """
    Construct an income and consumption series that is a combination of the two.
    """

    tb = tb.copy()

    # Pivot
    tb = pivot_table(
        tb=tb,
        index=["country", "year", "welfare_type"],
        columns=["ppp_version", "poverty_line", "decile"],
    )

    # Reset index in tb_both_inc_and_cons
    tb = tb.reset_index()

    # Sort values
    tb = tb.sort_values(by=["country", "year", "welfare_type"], ignore_index=True)

    # Flag duplicates per year – indicating multiple welfare_types
    tb["duplicate_flag"] = tb.duplicated(subset=[("country", "", "", ""), ("year", "", "", "")], keep=False)

    # Create a boolean column that is true if each country has only income or consumption
    tb["only_inc_or_cons"] = tb["welfare_type"].isnull() | tb.groupby(["country"])["welfare_type"].transform(
        lambda x: x.nunique() == 1
    )

    # Select only the rows with only income or consumption
    tb_only_inc_or_cons = tb[tb["only_inc_or_cons"]].reset_index(drop=True)

    # Create a table with the rest
    tb_both_inc_and_cons = tb[~tb["only_inc_or_cons"]].reset_index(drop=True)

    # Create a list of the countries with both income and consumption in the series
    countries_inc_cons = list(tb_both_inc_and_cons["country"].unique())

    # Assert that the countries with both income and consumption are expected
    unexpected_countries = set(countries_inc_cons) - set(COUNTRIES_WITH_INCOME_AND_CONSUMPTION)
    missing_countries = set(COUNTRIES_WITH_INCOME_AND_CONSUMPTION) - set(countries_inc_cons)
    assert not unexpected_countries and not missing_countries, log.fatal(
        f"Unexpected countries with both income and consumption: {unexpected_countries}. "
        f"Missing expected countries: {missing_countries}."
    )

    # Define empty table to store the smoothed series
    tb_both_inc_and_cons_smoothed = Table()
    for country in countries_inc_cons:
        # Filter country
        tb_country = tb_both_inc_and_cons[tb_both_inc_and_cons["country"] == country].reset_index(drop=True)

        # Save the max_year for the country
        max_year = tb_country["year"].max()

        # Define last_welfare_type for income and consumption. If both, list is saved as ['income', 'consumption']
        last_welfare_type = list(tb_country[tb_country["year"] == max_year]["welfare_type"].unique())
        last_welfare_type.sort()

        # Count how many times welfare_type switches from income to consumption and vice versa
        number_of_welfare_series = (
            (tb_country["welfare_type"] != tb_country["welfare_type"].shift(1).fillna("")).astype(int).cumsum().max()
        )

        # If there are only two welfare series, use one for these countries
        if number_of_welfare_series == 2:
            # assert if last_welfare type values are expected
            if country in ["Armenia", "Belarus", "Kyrgyzstan"]:
                welfare_expected = ["consumption"]
                assert len(last_welfare_type) == 1 and last_welfare_type == welfare_expected, log.fatal(
                    f"{country} has unexpected values of welfare_type: {last_welfare_type} instead of {welfare_expected}."
                )
                tb_country = tb_country[tb_country["welfare_type"].isin(last_welfare_type)].reset_index(drop=True)

            elif country in ["Kosovo", "North Macedonia", "Peru", "Uzbekistan"]:
                assert len(last_welfare_type) == 1 and last_welfare_type == ["income"], log.fatal(
                    f"{country} has unexpected values of welfare_type: {last_welfare_type} instead of ['income']"
                )
                tb_country = tb_country[tb_country["welfare_type"].isin(last_welfare_type)].reset_index(drop=True)

            # Don't do anything for the rest
            # [
            #     "China",
            #     "China (urban)",
            #     "China (rural)",
            #     "Kazakhstan",
            #     "Namibia",
            #     "Nepal",
            #     "Seychelles",
            # ]

        # With Turkey I want to keep both series, but there are duplicates for some years
        elif country in ["Turkey"]:
            welfare_expected = ["income"]
            assert len(last_welfare_type) == 1 and last_welfare_type == welfare_expected, log.fatal(
                f"{country} has unexpected values of welfare_type: {last_welfare_type} instead of {welfare_expected}"
            )

            tb_country = tb_country[
                (~tb_country["duplicate_flag"]) | (tb_country["welfare_type"].isin(last_welfare_type))
            ].reset_index(drop=True)

        # With Russia, though the last welfare type is income, I want to keep consumption, being the longest series
        elif country in ["Russia"]:
            welfare_expected = ["consumption"]
            assert len(last_welfare_type) == 1 and last_welfare_type != welfare_expected, log.fatal(
                f"For {country} we expect to use {welfare_expected}, which should be different from the last welfare type: {last_welfare_type}"
            )

            tb_country = tb_country[tb_country["welfare_type"].isin(welfare_expected)].reset_index(drop=True)

        # These are countries with both income and consumption as the last welfare type, so I decide case by case
        elif country in ["Haiti", "Philippines", "Saint Lucia"]:
            welfare_expected = ["consumption", "income"]
            assert len(last_welfare_type) == 2 and last_welfare_type == welfare_expected, log.fatal(
                f"{country} has unexpected values of welfare_type: {last_welfare_type} instead of {welfare_expected}"
            )
            if country in ["Haiti", "Saint Lucia"]:
                tb_country = tb_country[tb_country["welfare_type"] == "income"].reset_index(drop=True)
            elif country in ["Philippines"]:
                tb_country = tb_country[tb_country["welfare_type"] == "consumption"].reset_index(drop=True)

        else:
            # Here I keep the most recent welfare type
            if country in ["Albania", "Belize", "Ukraine"]:
                welfare_expected = ["consumption"]
                assert len(last_welfare_type) == 1 and last_welfare_type == welfare_expected, log.fatal(
                    f"{country} has unexpected values of welfare_type: {last_welfare_type} instead of {welfare_expected}."
                )
            elif country in [
                "Bulgaria",
                "Croatia",
                "Estonia",
                "Hungary",
                "Latvia",
                "Lithuania",
                "Montenegro",
                "Nicaragua",
                "Poland",
                "Romania",
                "Serbia",
                "Slovakia",
                "Slovenia",
            ]:
                welfare_expected = ["income"]
                assert len(last_welfare_type) == 1 and last_welfare_type == welfare_expected, log.fatal(
                    f"{country} has unexpected values of welfare_type: {last_welfare_type} instead of {welfare_expected}."
                )

            tb_country = tb_country[tb_country["welfare_type"].isin(last_welfare_type)].reset_index(drop=True)

        tb_both_inc_and_cons_smoothed = pr.concat([tb_both_inc_and_cons_smoothed, tb_country])

    # Drop the columns created in this function
    tb_both_inc_and_cons_smoothed = tb_both_inc_and_cons_smoothed.drop(
        columns=["only_inc_or_cons", "duplicate_flag"], errors="raise"
    )
    tb_only_inc_or_cons = tb_only_inc_or_cons.drop(columns=["only_inc_or_cons", "duplicate_flag"], errors="raise")

    # Restore the format of the table
    tb_both_inc_and_cons_smoothed = unpivot_table(
        tb=tb_both_inc_and_cons_smoothed,
        index=["country", "year", "welfare_type"],
        level=["ppp_version", "poverty_line", "decile"],
    )
    tb_only_inc_or_cons = unpivot_table(
        tb=tb_only_inc_or_cons,
        index=["country", "year", "welfare_type"],
        level=["ppp_version", "poverty_line", "decile"],
    )

    tb_inc_or_cons = pr.concat([tb_only_inc_or_cons, tb_both_inc_and_cons_smoothed], ignore_index=True)

    return tb_inc_or_cons


def check_jumps_in_grapher_dataset(tb: Table) -> None:
    """
    Check for jumps in the dataset, which can be caused by combining income and consumption estimates for one country series.
    """
    tb = tb.copy()

    # Pivot
    tb = pivot_table(
        tb=tb[["country", "year", "welfare_type", "ppp_version", "poverty_line", "decile", "headcount_ratio"]],
        index=["country", "year", "welfare_type"],
        columns=["ppp_version", "poverty_line", "decile"],
        join_column_levels_with="_",
    )

    # Reset index in tb
    tb = tb.reset_index()

    # For each country, year, welfare_type and reporting_level, check if the difference between the columns is too high
    # Define columns to check: all the headcount ratio columns
    cols_to_check = [col for col in tb.columns if "headcount_ratio" in col]

    # Among the columns to check, drop all the columns that only have missing values
    cols_to_check = [col for col in cols_to_check if tb[col].notnull().any()]

    for col in cols_to_check:
        # Create a new column, shift_col, that is the same as col but shifted one row down for each country, year, welfare_type and reporting_level
        tb["shift_col"] = tb.groupby(["country"])[col].shift(1)

        # Create shift_year column
        tb["shift_year"] = tb.groupby(["country"])["year"].shift(1)

        # Create shift_welfare_type column
        tb["shift_welfare_type"] = tb.groupby(["country"])["welfare_type"].shift(1)

        # Calculate the difference between col and shift_col
        tb["check_diff_column"] = tb[col] - tb["shift_col"]

        # Calculate the difference between years
        tb["check_diff_year"] = tb["year"] - tb["shift_year"]

        # Calculate if the welfare type is the same
        tb["check_diff_welfare_type"] = tb["welfare_type"] == tb["shift_welfare_type"]

        # Check if the difference is too high
        mask = (
            (abs(tb["check_diff_column"]) > 10)
            & (tb["check_diff_year"] <= 5)
            & ~tb["check_diff_welfare_type"].fillna(False)
        )
        tb_error = tb[mask].reset_index(drop=True)

        if not tb_error.empty:
            log.fatal(
                f"""There are {len(tb_error)} observations with abnormal jumps for {col}:
                {tabulate(tb_error[['ppp_version', 'country', 'year', col, 'check_diff_column', 'check_diff_year']].sort_values('year').reset_index(drop=True), headers = 'keys', tablefmt = TABLEFMT, floatfmt=".1f")}"""
            )
            # tb = tb[~mask].reset_index(drop=True)

    # Drop the columns created for the check
    tb = tb.drop(
        columns=[
            "shift_col",
            "shift_year",
            "shift_welfare_type",
            "check_diff_column",
            "check_diff_year",
            "check_diff_welfare_type",
        ],
        errors="raise",
    )

    return None


def survey_count(tb: Table) -> Table:
    """
    Create survey count indicator, by counting the number of surveys available for each country in the past decade
    """
    tb = tb.copy()

    # Remove regions from the table
    tb_survey = tb[~tb["country"].isin(REGIONS_LIST)].reset_index(drop=True)

    # Obtain the value of the second key in INTERNATIONAL_POVERTY_LINES
    # This is the value of the poverty line for the current year
    ipl_current = INTERNATIONAL_POVERTY_LINES[list(INTERNATIONAL_POVERTY_LINES.keys())[1]]

    # Filter for the current value
    tb_survey = tb_survey[tb_survey["poverty_line"] == ipl_current].reset_index(drop=True)

    min_year = int(tb_survey["year"].min())
    max_year = int(tb_survey["year"].max())
    year_list = list(range(min_year, max_year + 1))
    country_list = list(tb_survey["country"].unique())

    # Create two tables with all the years and entities
    year_tb_survey = Table(year_list)
    entity_tb_survey = Table(country_list)

    # Make a cartesian product of both dataframes: join all the combinations between all the entities and all the years
    cross = pr.merge(entity_tb_survey, year_tb_survey, how="cross")
    cross = cross.rename(columns={"0_x": "country", "0_y": "year"}, errors="raise")

    # Merge cross and tb_survey, to include all the possible rows in the dataset
    tb_survey = pr.merge(cross, tb_survey[["country", "year"]], on=["country", "year"], how="left", indicator=True)

    # Mark with 1 if there are surveys available, 0 if not (this is done by checking if the row is in both datasets)
    tb_survey["survey_available"] = 0
    tb_survey.loc[tb_survey["_merge"] == "both", "survey_available"] = 1

    # Sum for each entity the surveys available for the previous 9 years and the current year
    tb_survey["surveys_past_decade"] = (
        tb_survey["survey_available"]
        .groupby(tb_survey["country"], sort=False)
        .rolling(min_periods=1, window=10)
        .sum()
        .values
    )

    # Copy metadata
    tb_survey["surveys_past_decade"] = tb_survey["surveys_past_decade"].copy_metadata(tb["headcount"])

    # Keep columns needed
    tb_survey = tb_survey[["country", "year", "surveys_past_decade"]]

    # Define ppp_version and poverty_line columns, empty
    tb_survey["ppp_version"] = pd.NA
    tb_survey["poverty_line"] = pd.NA
    tb_survey["welfare_type"] = "income or consumption"
    tb_survey["table"] = "Income or consumption consolidated"
    tb_survey["decile"] = pd.NA

    # Merge with original table
    tb = pr.merge(
        tb_survey,
        tb,
        on=["country", "year", "welfare_type", "ppp_version", "poverty_line", "table", "decile"],
        how="outer",
    )

    return tb


def drop_columns(tb: Table) -> Table:
    """
    Drop columns not needed
    """

    # Remove columns
    tb = tb.drop(
        columns=[
            "is_interpolated",
            "reporting_pop",
            "reporting_gdp",
            "reporting_pce",
            "estimate_type",
            "pop_in_poverty",
        ],
        errors="raise",
    )

    return tb


def add_region_definitions(tb: Table, tb_region_definitions: Table) -> Table:
    """
    Add region definitions to the main table
    """

    tb_base = tb.copy()

    ipl_current = INTERNATIONAL_POVERTY_LINES[list(INTERNATIONAL_POVERTY_LINES.keys())[1]]

    # Filter for the current value
    tb_base = tb_base[tb_base["poverty_line"] == ipl_current].reset_index(drop=True)

    # Define ppp_version and poverty_line columns, empty
    tb_base["ppp_version"] = pd.NA
    tb_base["poverty_line"] = pd.NA

    # Do the same with tb_region_definitions
    tb_region_definitions["welfare_type"] = "income or consumption"
    tb_region_definitions["table"] = "Income or consumption consolidated"

    # Merge with original table
    tb_base = pr.merge(
        tb_base,
        tb_region_definitions,
        on=["country", "year", "welfare_type", "table"],
        how="outer",
    )

    # Now merge with tb
    tb = pr.merge(
        tb,
        tb_base[["country", "year", "welfare_type", "ppp_version", "poverty_line", "table", "region_name"]],
        on=["country", "year", "welfare_type", "ppp_version", "poverty_line", "table"],
        how="outer",
    )

    return tb


def make_distributional_indicators_long(tb: Table) -> Table:
    """
    Convert decile1, ..., decile10 and decile1_thr, ..., decile9_thr to a long format.
    """
    tb_base = tb.copy()

    # Extract both values from INTERNATIONAL_POVERTY_LINES. They are the values of the dictionary
    ipl_list = list(INTERNATIONAL_POVERTY_LINES.values())

    # Filter only for values in the list
    tb_base = tb_base[tb_base["poverty_line"].isin(ipl_list)].reset_index(drop=True)

    # Define index columns
    index_columns = ["country", "year", "welfare_type", "ppp_version", "filled"]

    # SHARE
    # Define share columns
    share_columns = [f"decile{i}_share" for i in range(1, 11)]
    tb_share = tb_base.melt(
        id_vars=index_columns,
        value_vars=share_columns,
        var_name="decile",
        value_name="share",
    )

    # THRESHOLD
    # Define threshold columns
    thr_columns = [f"decile{i}_thr" for i in range(1, 10)]
    tb_thr = tb_base.melt(
        id_vars=index_columns,
        value_vars=thr_columns,
        var_name="decile",
        value_name="thr",
    )

    # AVERAGE
    # Define average columns
    avg_columns = [f"decile{i}_avg" for i in range(1, 11)]
    tb_avg = tb_base.melt(
        id_vars=index_columns,
        value_vars=avg_columns,
        var_name="decile",
        value_name="avg",
    )

    # Merge newly created tables
    tb_distributional = pr.multi_merge(
        [tb_share, tb_thr, tb_avg],
        on=index_columns + ["decile"],
        how="outer",
    )

    # Remove "decile" from the decile column
    tb_distributional["decile"] = tb_distributional["decile"].str.replace("decile", "")

    # Do the same with "_share", "_thr", and "_avg"
    for indicator in ["share", "thr", "avg"]:
        tb_distributional["decile"] = tb_distributional["decile"].str.replace(f"_{indicator}", "")

    # Group the rows by country, year, welfare_type, ppp_version, and decile
    tb_distributional = (
        tb_distributional.groupby(
            index_columns + ["decile"],
            as_index=False,
            dropna=False,
        )
        .agg(
            {
                "share": "first",
                "thr": "first",
                "avg": "first",
            }
        )
        .reset_index(drop=True)
    )

    # Create an empty decile column in tb
    tb["decile"] = pd.NA

    # Concatenate tb and tb_distributional
    tb = pr.concat([tb, tb_distributional], ignore_index=True)

    # Remove share_columns and threshold_columns
    tb = tb.drop(columns=share_columns + thr_columns + avg_columns, errors="raise")

    return tb


def make_relative_poverty_long(tb: Table) -> Table:
    """
    Convert relative poverty columns to a long format.
    """
    tb_relative = tb.copy()

    # Extract both values from INTERNATIONAL_POVERTY_LINES. They are the values of the dictionary
    ipl_list = list(INTERNATIONAL_POVERTY_LINES.values())

    # Filter only for values in the list
    tb_relative = tb_relative[tb_relative["poverty_line"].isin(ipl_list)].reset_index(drop=True)

    # Define index columns
    index_columns = ["country", "year", "welfare_type", "ppp_version", "filled"]

    # Define relative poverty columns. They are all the columns that contain "_median"
    rel_pov_columns = [col for col in tb.columns if "_median" in col]

    # Melt the table
    tb_relative = tb_relative.melt(
        id_vars=index_columns,
        value_vars=rel_pov_columns,
        var_name="indicator",
        value_name="value",
    )

    # Split the indicator column into two columns: indicator and poverty_line. Poverty line would be in the format "40_median", "50_median", or "60_median"
    tb_relative[["indicator", "poverty_line"]] = tb_relative["indicator"].str.extract(
        r"(.+?)_(\d+_median)", expand=True
    )

    # In poverty_line, replace "_median" with "% of the median"
    tb_relative["poverty_line"] = tb_relative["poverty_line"].str.replace("_median", "% of the median")

    # Make tb_relative wide, by pivoting the indicator column
    tb_relative = tb_relative.pivot(
        index=index_columns + ["poverty_line"],
        columns="indicator",
        values="value",
    ).reset_index()

    # Concatenate with original table
    tb = pr.concat([tb, tb_relative], ignore_index=True)

    # Drop the columns that are not needed
    tb = tb.drop(columns=rel_pov_columns, errors="raise")

    return tb


def make_poverty_line_null_for_non_dimensional_indicators(tb: Table) -> Table:
    """
    Avoid repetition of the same values for indicators not depending on poverty lines.
    """

    tb_non_dimensional = tb.copy()

    # Extract both values from INTERNATIONAL_POVERTY_LINES. They are the values of the dictionary
    ipl_list = list(INTERNATIONAL_POVERTY_LINES.values())

    # Filter only for values in the list
    tb_non_dimensional = tb_non_dimensional[
        (tb_non_dimensional["poverty_line"].isin(ipl_list)) & tb_non_dimensional["decile"].isna()
    ].reset_index(drop=True)

    # Define index columns
    index_columns = ["country", "year", "welfare_type", "ppp_version", "filled", "poverty_line", "decile"]

    # Select the columns we want
    tb_non_dimensional = tb_non_dimensional[index_columns + INDICATORS_NOT_DEPENDENT_ON_POVLINES_NOR_DECILES]

    # Add a missing poverty_line and decile columns
    tb_non_dimensional["poverty_line"] = pd.NA

    # Drop the columns in tb
    tb = tb.drop(columns=INDICATORS_NOT_DEPENDENT_ON_POVLINES_NOR_DECILES, errors="raise")

    # Concatenate tb and tb_non_dimensional
    tb = pr.merge(tb, tb_non_dimensional, on=index_columns, how="outer")

    return tb


def make_cpi_not_depending_on_ppp(tb: Table) -> Table:
    """
    Make the cpi not depending on ppp_version.
    """

    tb = tb.copy()

    # Extract last key from POVLINES_DICT, so we can filter for the latest ppp year
    current_ppp_year = list(POVLINES_DICT.keys())[-1]

    # Make all the cpi values different from the current ppp year None
    tb.loc[tb["ppp_version"] != current_ppp_year, "cpi"] = pd.NA

    return tb


def separate_filled_and_unfilled_data(tb: Table) -> Tuple[Table, Table]:
    """
    Separate filled and unfilled data.

    Sometimes, some data processing is needed only for unfilled data and we need to separate the data to achieve that.
    As I am doing it several times in the script, I prefer to create a function for that.
    """

    tb_filled = tb[tb["filled"]].copy()
    tb_unfilled = tb[~tb["filled"]].copy()

    return tb_filled, tb_unfilled

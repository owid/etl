"""
Load a meadow dataset and create a garden dataset.

When running this step in an update, be sure to check all the outputs and logs to ensure the data is correct.

NOTE: To extract the log of the process (to review sanity checks, for example), run the following command in the terminal:
    nohup uv run etl run world_bank_pip > output.log 2>&1 &
"""

from typing import Tuple

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Table
from shared import add_metadata_vars, add_metadata_vars_percentiles
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
# TODO: Modify the lines in 2021 prices
POVLINES_DICT = {
    2017: [100, 215, 365, 685, 1000, 2000, 3000, 4000],
    2021: [100, 300, 420, 830, 1000, 2000, 3000, 4000],
}

# Define PPP versions from POVLINES_DICT
PPP_VERSIONS = list(POVLINES_DICT.keys())

PPP_YEAR_OLD = PPP_VERSIONS[0]
PPP_YEAR_CURRENT = PPP_VERSIONS[1]

# Define current International Poverty Line (in cents)
# Define international poverty lines as the second value in each list in POVLINES_DICT
INTERNATIONAL_POVERTY_LINES = {ppp_year: poverty_lines[1] for ppp_year, poverty_lines in POVLINES_DICT.items()}

# Define current International Poverty Line (IPL) in the latest prices
INTERNATIONAL_POVERTY_LINE_CURRENT = INTERNATIONAL_POVERTY_LINES[PPP_VERSIONS[1]]

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

    # Process data
    # Make table wide and change column names
    tb = process_data(tb)

    # Calculate inequality measures
    tb = calculate_inequality(tb)

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
    tb = regional_data_from_1990(tb, REGIONS_LIST)
    tb_percentiles = regional_data_from_1990(tb_percentiles, REGIONS_LIST)

    # Amend the entity to reflect if data refers to urban or rural only
    tb = identify_rural_urban(tb)

    # Separate out ppp and filled data from the main dataset
    tb_ppp_old, tb_ppp_current = separate_ppp_data(tb)
    tb_percentiles_ppp_old, tb_percentiles_ppp_current = separate_ppp_data(tb_percentiles)

    # Create stacked variables from headcount and headcount_ratio
    tb_ppp_old, col_stacked_n_ppp_old, col_stacked_pct_ppp_old = create_stacked_variables(
        tb_ppp_old, POVLINES_DICT, ppp_version=PPP_YEAR_OLD
    )
    tb_ppp_current, col_stacked_n_ppp_current, col_stacked_pct_ppp_current = create_stacked_variables(
        tb_ppp_current, POVLINES_DICT, ppp_version=PPP_YEAR_CURRENT
    )

    # Sanity checks. I don't run for percentile tables because that process was done in the extraction
    tb_ppp_old = sanity_checks(
        tb_ppp_old,
        POVLINES_DICT,
        ppp_version=PPP_YEAR_OLD,
        col_stacked_n=col_stacked_n_ppp_old,
        col_stacked_pct=col_stacked_pct_ppp_old,
    )
    tb_ppp_current = sanity_checks(
        tb_ppp_current,
        POVLINES_DICT,
        ppp_version=PPP_YEAR_CURRENT,
        col_stacked_n=col_stacked_n_ppp_current,
        col_stacked_pct=col_stacked_pct_ppp_current,
    )

    # Separate out consumption-only, income-only. Also, create a table with both income and consumption
    tb_inc_ppp_old, tb_cons_ppp_old, tb_inc_or_cons_ppp_old_unsmoothed, tb_inc_or_cons_ppp_old = inc_or_cons_data(
        tb_ppp_old
    )
    tb_inc_ppp_current, tb_cons_ppp_current, tb_inc_or_cons_ppp_current_unsmoothed, tb_inc_or_cons_ppp_current = (
        inc_or_cons_data(tb_ppp_current)
    )

    # Create regional headcount variable, by patching missing values with the difference between world and regional headcount
    tb_inc_or_cons_ppp_current = regional_headcount(tb_inc_or_cons_ppp_current)

    # Create survey count dataset, by counting the number of surveys available for each country in the past decade
    tb_inc_or_cons_ppp_current = survey_count(tb_inc_or_cons_ppp_current)

    # Add region definitions
    tb_inc_or_cons_ppp_current = add_region_definitions(
        tb=tb_inc_or_cons_ppp_current, tb_region_definitions=tb_region_definitions
    )

    # Add metadata by code
    tb_inc_ppp_old = add_metadata_vars(tb_garden=tb_inc_ppp_old, ppp_version=PPP_YEAR_OLD, welfare_type="income")
    tb_cons_ppp_old = add_metadata_vars(tb_garden=tb_cons_ppp_old, ppp_version=PPP_YEAR_OLD, welfare_type="consumption")
    tb_inc_or_cons_ppp_old_unsmoothed = add_metadata_vars(
        tb_garden=tb_inc_or_cons_ppp_old_unsmoothed,
        ppp_version=PPP_YEAR_OLD,
        welfare_type="income_consumption",
    )
    tb_inc_or_cons_ppp_old_unsmoothed.m.short_name = f"income_consumption_{PPP_YEAR_OLD}_unsmoothed"
    tb_inc_or_cons_ppp_old = add_metadata_vars(
        tb_garden=tb_inc_or_cons_ppp_old,
        ppp_version=PPP_YEAR_OLD,
        welfare_type="income_consumption",
    )

    tb_inc_ppp_current = add_metadata_vars(
        tb_garden=tb_inc_ppp_current, ppp_version=PPP_YEAR_CURRENT, welfare_type="income"
    )
    tb_cons_ppp_current = add_metadata_vars(
        tb_garden=tb_cons_ppp_current, ppp_version=PPP_YEAR_CURRENT, welfare_type="consumption"
    )
    tb_inc_or_cons_ppp_current_unsmoothed = add_metadata_vars(
        tb_garden=tb_inc_or_cons_ppp_current_unsmoothed,
        ppp_version=PPP_YEAR_CURRENT,
        welfare_type="income_consumption",
    )
    tb_inc_or_cons_ppp_current_unsmoothed.m.short_name = f"income_consumption_{PPP_YEAR_CURRENT}_unsmoothed"
    tb_inc_or_cons_ppp_current = add_metadata_vars(
        tb_garden=tb_inc_or_cons_ppp_current,
        ppp_version=PPP_YEAR_CURRENT,
        welfare_type="income_consumption",
    )

    tb_percentiles_ppp_old = add_metadata_vars_percentiles(
        tb_garden=tb_percentiles_ppp_old,
        ppp_version=PPP_YEAR_OLD,
        welfare_type="income_consumption",
    )
    tb_percentiles_ppp_current = add_metadata_vars_percentiles(
        tb_garden=tb_percentiles_ppp_current,
        ppp_version=PPP_YEAR_CURRENT,
        welfare_type="income_consumption",
    )

    # Set index and sort
    # Define index cols
    index_cols = ["country", "year"]
    index_cols_unsmoothed = ["country", "year", "reporting_level", "welfare_type"]
    index_cols_percentiles = ["country", "year", "reporting_level", "welfare_type", "percentile"]
    tb_inc_ppp_old = tb_inc_ppp_old.format(keys=index_cols)
    tb_cons_ppp_old = tb_cons_ppp_old.format(keys=index_cols)
    tb_inc_or_cons_ppp_old_unsmoothed = tb_inc_or_cons_ppp_old_unsmoothed.format(keys=index_cols_unsmoothed)
    tb_inc_or_cons_ppp_old = tb_inc_or_cons_ppp_old.format(keys=index_cols)

    tb_inc_ppp_current = tb_inc_ppp_current.format(keys=index_cols)
    tb_cons_ppp_current = tb_cons_ppp_current.format(keys=index_cols)
    tb_inc_or_cons_ppp_current_unsmoothed = tb_inc_or_cons_ppp_current_unsmoothed.format(keys=index_cols_unsmoothed)
    tb_inc_or_cons_ppp_current = tb_inc_or_cons_ppp_current.format(keys=index_cols)

    tb_percentiles_ppp_old = tb_percentiles_ppp_old.format(keys=index_cols_percentiles)
    tb_percentiles_ppp_current = tb_percentiles_ppp_current.format(keys=index_cols_percentiles)

    # Create spell tables to separate different survey spells in the explorers
    spell_tables_inc = create_survey_spells(tb=tb_inc_ppp_current)
    spell_tables_cons = create_survey_spells(tb=tb_cons_ppp_current)

    # For income and consumption we combine the tables to not lose information from tb_inc_or_cons_ppp_current
    spell_tables_inc_or_cons = create_survey_spells_inc_cons(tb_inc=tb_inc_ppp_current, tb_cons=tb_cons_ppp_current)

    # Drop columns not needed
    tb_inc_ppp_old = drop_columns(tb_inc_ppp_old)
    tb_cons_ppp_old = drop_columns(tb_cons_ppp_old)
    tb_inc_or_cons_ppp_old = drop_columns(tb_inc_or_cons_ppp_old)

    tb_inc_ppp_current = drop_columns(tb_inc_ppp_current)
    tb_cons_ppp_current = drop_columns(tb_cons_ppp_current)
    tb_inc_or_cons_ppp_current = drop_columns(tb_inc_or_cons_ppp_current)

    # Merge tables for PPP comparison explorer
    tb_inc_ppp_comparison = combine_tables_ppp_comparison(
        tb_ppp_old=tb_inc_ppp_old,
        tb_ppp_current=tb_inc_ppp_current,
        short_name=f"income_{PPP_YEAR_OLD}_{PPP_YEAR_CURRENT}",
    )
    tb_cons_ppp_comparison = combine_tables_ppp_comparison(
        tb_ppp_old=tb_cons_ppp_old,
        tb_ppp_current=tb_cons_ppp_current,
        short_name=f"consumption_{PPP_YEAR_OLD}_{PPP_YEAR_CURRENT}",
    )
    tb_inc_or_cons_ppp_comparison = combine_tables_ppp_comparison(
        tb_ppp_old=tb_inc_or_cons_ppp_old,
        tb_ppp_current=tb_inc_or_cons_ppp_current,
        short_name=f"income_consumption_{PPP_YEAR_OLD}_{PPP_YEAR_CURRENT}",
    )

    # Define tables to upload
    # The ones we need in Grapher admin would be tb_inc_or_cons_ppp_old, tb_inc_or_cons_ppp_current
    tables = (
        [
            tb_inc_ppp_old,
            tb_cons_ppp_old,
            tb_inc_or_cons_ppp_old_unsmoothed,
            tb_inc_or_cons_ppp_old,
            tb_inc_ppp_current,
            tb_cons_ppp_current,
            tb_inc_or_cons_ppp_current_unsmoothed,
            tb_inc_or_cons_ppp_current,
            tb_inc_ppp_comparison,
            tb_cons_ppp_comparison,
            tb_inc_or_cons_ppp_comparison,
            tb_percentiles_ppp_old,
            tb_percentiles_ppp_current,
        ]
        + spell_tables_inc
        + spell_tables_cons
        + spell_tables_inc_or_cons
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def process_data(tb: Table) -> Table:
    # rename columns
    tb = tb.rename(columns={"headcount": "headcount_ratio", "poverty_gap": "poverty_gap_index"}, errors="raise")

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

    # Create a new column for the poverty line in cents and string
    tb["poverty_line_cents"] = round(tb["poverty_line"] * 100).astype(int).astype(str)

    # Make the table wide, with poverty_line_cents as columns
    tb = tb.pivot(
        index=[
            "ppp_version",
            "country",
            "year",
            "reporting_level",
            "welfare_type",
            "survey_comparability",
            "comparable_spell",
            "reporting_pop",
            "mean",
            "median",
            "mld",
            "gini",
            "polarization",
            "decile1_share",
            "decile2_share",
            "decile3_share",
            "decile4_share",
            "decile5_share",
            "decile6_share",
            "decile7_share",
            "decile8_share",
            "decile9_share",
            "decile10_share",
            "decile1_thr",
            "decile2_thr",
            "decile3_thr",
            "decile4_thr",
            "decile5_thr",
            "decile6_thr",
            "decile7_thr",
            "decile8_thr",
            "decile9_thr",
            "is_interpolated",
            "distribution_type",
            "estimation_type",
            "headcount_40_median",
            "headcount_50_median",
            "headcount_60_median",
            "headcount_ratio_40_median",
            "headcount_ratio_50_median",
            "headcount_ratio_60_median",
            "income_gap_ratio_40_median",
            "income_gap_ratio_50_median",
            "income_gap_ratio_60_median",
            "poverty_gap_index_40_median",
            "poverty_gap_index_50_median",
            "poverty_gap_index_60_median",
            "avg_shortfall_40_median",
            "avg_shortfall_50_median",
            "avg_shortfall_60_median",
            "total_shortfall_40_median",
            "total_shortfall_50_median",
            "total_shortfall_60_median",
            "poverty_severity_40_median",
            "poverty_severity_50_median",
            "poverty_severity_60_median",
            "watts_40_median",
            "watts_50_median",
            "watts_60_median",
            "spl",
            "spr",
            "pg",
            "cpi",
            "ppp",
        ],
        columns="poverty_line_cents",
        values=[
            "headcount",
            "headcount_ratio",
            "income_gap_ratio",
            "poverty_gap_index",
            "avg_shortfall",
            "total_shortfall",
            "poverty_severity",
            "watts",
        ],
    )

    # Flatten column names
    tb.columns = ["_".join(col).strip() for col in tb.columns.values]

    # Reset index
    tb = tb.reset_index()

    return tb


def create_stacked_variables(tb: Table, povlines_dict: dict, ppp_version: int) -> Tuple[Table, list, list]:
    """
    Create stacked variables from the indicators to plot them as stacked area/bar charts
    """
    # Select poverty lines between PPP_YEAR_OLD and PPP_YEAR_CURRENT and sort in case they are not in order
    povlines = povlines_dict[ppp_version]
    povlines.sort()

    # Above variables

    col_above_n = []
    col_above_pct = []

    for p in povlines:
        varname_n = f"headcount_above_{p}"
        varname_pct = f"headcount_ratio_above_{p}"

        tb[varname_n] = tb["reporting_pop"] - tb[f"headcount_{p}"]
        tb[varname_pct] = tb[varname_n] / tb["reporting_pop"]

        col_above_n.append(varname_n)
        col_above_pct.append(varname_pct)

    tb.loc[:, col_above_pct] = tb[col_above_pct] * 100

    # Stacked variables

    col_stacked_n = []
    col_stacked_pct = []

    for i in range(len(povlines)):
        # if it's the first value only continue
        if i == 0:
            continue

        # If it's the last value calculate the people between this value and the previous
        # and also the people over this poverty line (and percentages)
        elif i == len(povlines) - 1:
            varname_n = f"headcount_between_{povlines[i-1]}_{povlines[i]}"
            varname_pct = f"headcount_ratio_between_{povlines[i-1]}_{povlines[i]}"
            tb[varname_n] = tb[f"headcount_{povlines[i]}"] - tb[f"headcount_{povlines[i-1]}"]
            tb[varname_pct] = tb[varname_n] / tb["reporting_pop"]
            col_stacked_n.append(varname_n)
            col_stacked_pct.append(varname_pct)
            varname_n = f"headcount_above_{povlines[i]}"
            varname_pct = f"headcount_ratio_above_{povlines[i]}"
            tb[varname_n] = tb["reporting_pop"] - tb[f"headcount_{povlines[i]}"]
            tb[varname_pct] = tb[varname_n] / tb["reporting_pop"]
            col_stacked_n.append(varname_n)
            col_stacked_pct.append(varname_pct)

        # If it's any value between the first and the last calculate the people between this value and the previous (and percentage)
        else:
            varname_n = f"headcount_between_{povlines[i-1]}_{povlines[i]}"
            varname_pct = f"headcount_ratio_between_{povlines[i-1]}_{povlines[i]}"
            tb[varname_n] = tb[f"headcount_{povlines[i]}"] - tb[f"headcount_{povlines[i-1]}"]
            tb[varname_pct] = tb[varname_n] / tb["reporting_pop"]
            col_stacked_n.append(varname_n)
            col_stacked_pct.append(varname_pct)

    tb.loc[:, col_stacked_pct] = tb[col_stacked_pct] * 100

    # Add variables below first poverty line to the stacked variables
    col_stacked_n.append(f"headcount_{povlines[0]}")
    col_stacked_pct.append(f"headcount_ratio_{povlines[0]}")

    # Calculate stacked variables which "jump" the original order

    tb[f"headcount_between_{povlines[1]}_{povlines[4]}"] = (
        tb[f"headcount_{povlines[4]}"] - tb[f"headcount_{povlines[1]}"]
    )
    tb[f"headcount_between_{povlines[4]}_{povlines[6]}"] = (
        tb[f"headcount_{povlines[6]}"] - tb[f"headcount_{povlines[4]}"]
    )

    tb[f"headcount_ratio_between_{povlines[1]}_{povlines[4]}"] = (
        tb[f"headcount_ratio_{povlines[4]}"] - tb[f"headcount_ratio_{povlines[1]}"]
    )
    tb[f"headcount_ratio_between_{povlines[4]}_{povlines[6]}"] = (
        tb[f"headcount_ratio_{povlines[6]}"] - tb[f"headcount_ratio_{povlines[4]}"]
    )

    return tb, col_stacked_n, col_stacked_pct


def calculate_inequality(tb: Table) -> Table:
    """
    Calculate inequality measures: decile averages and ratios
    """

    col_decile_share = []
    col_decile_avg = []
    col_decile_thr = []

    for i in range(1, 11):
        if i != 10:
            varname_thr = f"decile{i}_thr"
            col_decile_thr.append(varname_thr)

        varname_share = f"decile{i}_share"
        varname_avg = f"decile{i}_avg"
        tb[varname_avg] = tb[varname_share] * tb["mean"] / 0.1

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
    tb = tb.replace([np.inf, -np.inf], np.nan)
    return tb


def identify_rural_urban(tb: Table) -> Table:
    """
    Amend the entity to reflect if data refers to urban or rural only
    """

    # Make country and reporting_level columns into strings
    tb["country"] = tb["country"].astype(str)
    tb["reporting_level"] = tb["reporting_level"].astype(str)
    ix = tb["reporting_level"].isin(["urban", "rural"])
    tb.loc[(ix), "country"] = tb.loc[(ix), "country"] + " (" + tb.loc[(ix), "reporting_level"] + ")"

    return tb


def sanity_checks(
    tb: Table,
    povlines_dict: dict,
    ppp_version: int,
    col_stacked_n: list,
    col_stacked_pct: list,
) -> Table:
    """
    Sanity checks for the table
    """

    # Select poverty lines between PPP_YEAR_OLD and PPP_YEAR_CURRENT and sort in case they are not in order
    povlines = povlines_dict[ppp_version]
    povlines.sort()

    # Save the number of observations before the checks
    obs_before_checks = len(tb)

    # Create lists of variables to check
    col_headcount = []
    col_headcount_ratio = []
    col_povertygap = []
    col_tot_shortfall = []
    col_watts = []
    col_poverty_severity = []
    col_decile_share = []
    col_decile_thr = []

    for p in povlines:
        col_headcount.append(f"headcount_{p}")
        col_headcount_ratio.append(f"headcount_ratio_{p}")
        col_povertygap.append(f"poverty_gap_index_{p}")
        col_tot_shortfall.append(f"total_shortfall_{p}")
        col_watts.append(f"watts_{p}")
        col_poverty_severity.append(f"poverty_severity_{p}")

    for i in range(1, 11):
        col_decile_share.append(f"decile{i}_share")
        if i != 10:
            col_decile_thr.append(f"decile{i}_thr")

    ############################
    # Negative values
    mask = (
        tb[
            col_headcount
            + col_headcount_ratio
            + col_povertygap
            + col_tot_shortfall
            + col_watts
            + col_poverty_severity
            + col_decile_share
            + col_decile_thr
            + ["mean", "median", "mld", "gini", "polarization"]
        ]
        .lt(0)
        .any(axis=1)
    )
    tb_error = tb[mask].reset_index(drop=True)

    if not tb_error.empty:
        log.fatal(
            f"""There are {len(tb_error)} observations with negative values! In
            {tabulate(tb_error[['country', 'year', 'reporting_level', 'welfare_type']], headers = 'keys', tablefmt = TABLEFMT)}"""
        )
        # NOTE: Check if we want to delete these observations
        # tb = tb[~mask].reset_index(drop=True)

    ############################
    # stacked values not adding up to 100%
    tb["sum_pct"] = tb[col_stacked_pct].sum(axis=1)
    mask = (tb["sum_pct"] >= 100.1) | (tb["sum_pct"] <= 99.9)
    tb_error = tb[mask].reset_index(drop=True)

    if not tb_error.empty:
        if DEBUG:
            log.warning(
                f"""{len(tb_error)} observations of stacked values are not adding up to 100% and will be deleted:
                {tabulate(tb_error[['country', 'year', 'reporting_level', 'welfare_type', 'sum_pct']], headers = 'keys', tablefmt = TABLEFMT, floatfmt=".1f")}"""
            )
        tb = tb[~mask].reset_index(drop=True)

    ############################
    # missing poverty values (headcount, poverty gap, total shortfall)
    cols_to_check = (
        col_headcount + col_headcount_ratio + col_povertygap + col_tot_shortfall + col_stacked_n + col_stacked_pct
    )
    mask = (tb[cols_to_check].isna().any(axis=1)) & (
        ~tb["country"].isin(["World (excluding China)", "World (excluding India)"])
    )
    tb_error = tb[mask].reset_index(drop=True)

    if not tb_error.empty:
        if DEBUG:
            log.warning(
                f"""There are {len(tb_error)} observations with missing poverty values and will be deleted:
                {tabulate(tb_error[['country', 'year', 'reporting_level', 'welfare_type'] + col_headcount], headers = 'keys', tablefmt = TABLEFMT)}"""
            )
        tb = tb[~mask].reset_index(drop=True)

    ############################
    # headcount monotonicity check
    m_check_vars = []
    for i in range(len(col_headcount)):
        if i > 0:
            check_varname = f"m_check_{i}"
            tb[check_varname] = tb[f"{col_headcount[i]}"] >= tb[f"{col_headcount[i-1]}"]
            m_check_vars.append(check_varname)
    tb["check_total"] = tb[m_check_vars].all(axis=1)

    tb_error = tb[~tb["check_total"]].reset_index(drop=True)

    if not tb_error.empty:
        if DEBUG:
            log.warning(
                f"""There are {len(tb_error)} observations with headcount not monotonically increasing and will be deleted:
                {tabulate(tb_error[['country', 'year', 'reporting_level', 'welfare_type'] + col_headcount], headers = 'keys', tablefmt = TABLEFMT, floatfmt="0.0f")}"""
            )
        tb = tb[tb["check_total"]].reset_index(drop=True)

    ############################
    # Threshold monotonicity check
    m_check_vars = []
    for i in range(1, 10):
        if i > 1:
            check_varname = f"m_check_{i}"
            tb[check_varname] = tb[f"decile{i}_thr"] >= tb[f"decile{i-1}_thr"]
            m_check_vars.append(check_varname)

    tb["check_total"] = tb[m_check_vars].all(axis=1)

    # Drop rows if columns in col_decile_thr are all null. Keep if some are null
    mask = (~tb["check_total"]) & (tb[col_decile_thr].notnull().any(axis=1))

    tb_error = tb[mask].reset_index(drop=True)

    if not tb_error.empty:
        if DEBUG:
            log.warning(
                f"""There are {len(tb_error)} observations with thresholds not monotonically increasing and will be deleted:
                {tabulate(tb_error[['country', 'year', 'reporting_level', 'welfare_type']], headers = 'keys', tablefmt = TABLEFMT)}"""
            )
        tb = tb[~mask].reset_index(drop=True)

    ############################
    # Shares monotonicity check
    m_check_vars = []
    for i in range(1, 11):
        if i > 1:
            check_varname = f"m_check_{i}"
            tb[check_varname] = tb[f"decile{i}_share"] >= tb[f"decile{i-1}_share"]
            m_check_vars.append(check_varname)

    tb["check_total"] = tb[m_check_vars].all(axis=1)

    # Drop rows if columns in col_decile_share are all null. Keep if some are null
    mask = (~tb["check_total"]) & (tb[col_decile_share].notnull().any(axis=1))
    tb_error = tb[mask].reset_index(drop=True)

    if not tb_error.empty:
        if DEBUG:
            log.warning(
                f"""There are {len(tb_error)} observations with shares not monotonically increasing and will be deleted:
                {tabulate(tb_error[['country', 'year', 'reporting_level', 'welfare_type'] + col_decile_share], headers = 'keys', tablefmt = TABLEFMT, floatfmt=".1f")}"""
            )
        tb = tb[~mask].reset_index(drop=True)

    ############################
    # Shares not adding up to 100%

    tb["sum_pct"] = tb[col_decile_share].sum(axis=1)

    # Drop rows if columns in col_decile_share are all null. Keep if some are null
    mask = (tb["sum_pct"] >= 100.1) | (tb["sum_pct"] <= 99.9) & (tb[col_decile_share].notnull().any(axis=1))
    tb_error = tb[mask].reset_index(drop=True)

    if not tb_error.empty:
        if DEBUG:
            log.warning(
                f"""{len(tb_error)} observations of shares are not adding up to 100% and will be deleted:
                {tabulate(tb_error[['country', 'year', 'reporting_level', 'welfare_type', 'sum_pct']], headers = 'keys', tablefmt = TABLEFMT, floatfmt=".1f")}"""
            )
        tb = tb[~mask].reset_index(drop=True)

    ############################
    # Shares not adding up to 100% (top 1%)

    # Define columns to add up to 100%
    col_decile_share_top = ["bottom50_share", "middle40_share", "top90_99_share", "top1_share"]

    tb["sum_pct"] = tb[col_decile_share_top].sum(axis=1)

    # Drop rows if columns in col_decile_share_top are all null. Keep if some are null
    mask = (tb["sum_pct"] >= 100.1) | (tb["sum_pct"] <= 99.9) & (tb[col_decile_share_top].notnull().any(axis=1))
    tb_error = tb[mask].reset_index(drop=True)

    if not tb_error.empty:
        if DEBUG:
            log.warning(
                f"""{len(tb_error)} observations of shares (with top 1%) are not adding up to 100% and will be converted to null:
                {tabulate(tb_error[['country', 'year', 'reporting_level', 'welfare_type', 'sum_pct']], headers = 'keys', tablefmt = TABLEFMT, floatfmt=".1f")}"""
            )
        # Make columns None if mask is True
        tb.loc[mask, ["top90_99_share", "top1_share"]] = None

    ############################
    # delete columns created for the checks
    tb = tb.drop(columns=m_check_vars + ["m_check_1", "check_total", "sum_pct"], errors="raise")

    obs_after_checks = len(tb)
    log.info(f"Sanity checks deleted {obs_before_checks - obs_after_checks} observations for {ppp_version} PPPs.")

    return tb


def separate_ppp_data(tb: Table) -> Tuple[Table, Table]:
    """
    Separate out ppp data from the main dataset
    """

    # Filter table to include only the right ppp_version
    # Also, drop columns with all NaNs (which are the ones that are not relevant for the ppp_version)
    tb_ppp_old = tb[tb["ppp_version"] == PPP_YEAR_OLD].dropna(axis=1, how="all").reset_index(drop=True)
    tb_ppp_current = tb[tb["ppp_version"] == PPP_YEAR_CURRENT].dropna(axis=1, how="all").reset_index(drop=True)

    return tb_ppp_old, tb_ppp_current


def inc_or_cons_data(tb: Table) -> Tuple[Table, Table, Table, Table]:
    """
    Separate income and consumption data
    """

    # Separate out consumption-only, income-only. Also, create a table with both income and consumption
    tb_inc = tb[tb["welfare_type"] == "income"].reset_index(drop=True)
    tb_cons = tb[tb["welfare_type"] == "consumption"].reset_index(drop=True)
    tb_inc_or_cons = tb.copy()
    tb_inc_or_cons_unsmoothed = tb.copy()

    tb_inc_or_cons = create_smooth_inc_cons_series(tb_inc_or_cons)

    tb_inc_or_cons = check_jumps_in_grapher_dataset(tb_inc_or_cons)

    return tb_inc, tb_cons, tb_inc_or_cons_unsmoothed, tb_inc_or_cons


def create_smooth_inc_cons_series(tb: Table) -> Table:
    """
    Construct an income and consumption series that is a combination of the two.
    """

    tb = tb.copy()

    # Flag duplicates per year – indicating multiple welfare_types
    # Sort values to ensure the welfare_type consumption is marked as False when there are multiple welfare types
    tb = tb.sort_values(by=["country", "year", "welfare_type"], ignore_index=True)
    tb["duplicate_flag"] = tb.duplicated(subset=["country", "year"], keep=False)

    # Create a boolean column that is true if each ppp_version, country, reporting_level has only income or consumption
    tb["only_inc_or_cons"] = tb["welfare_type"].isnull() | (
        tb.groupby(["country"])["welfare_type"].transform(lambda x: x.nunique() == 1)
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

        # Define welfare_type for income and consumption. If both, list is saved as ['income', 'consumption']
        last_welfare_type = list(tb_country[tb_country["year"] == max_year]["welfare_type"].unique())
        last_welfare_type.sort()

        # Count how many times welfare_type switches from income to consumption and vice versa
        number_of_welfare_series = (
            (tb_country["welfare_type"] != tb_country["welfare_type"].shift(1)).astype(float).cumsum().max()
        )

        # If there are only two welfare series, use one for these countries
        if number_of_welfare_series == 1:
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

        # With Turkey I also want to keep both series, but there are duplicates for some years
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

    tb_inc_or_cons = pr.concat([tb_only_inc_or_cons, tb_both_inc_and_cons_smoothed], ignore_index=True)

    # Drop the columns created in this function
    tb_inc_or_cons = tb_inc_or_cons.drop(columns=["only_inc_or_cons", "duplicate_flag"], errors="raise")

    return tb_inc_or_cons


def check_jumps_in_grapher_dataset(tb: Table) -> Table:
    """
    Check for jumps in the dataset, which can be caused by combining income and consumption estimates for one country series.
    """
    tb = tb.copy()

    # For each country, year, welfare_type and reporting_level, check if the difference between the columns is too high

    # Define columns to check: all the headcount ratio columns
    cols_to_check = [
        col for col in tb.columns if "headcount_ratio" in col and "above" not in col and "between" not in col
    ]

    for col in cols_to_check:
        # Create a new column, shift_col, that is the same as col but shifted one row down for each country, year, welfare_type and reporting_level
        tb["shift_col"] = tb.groupby(["country", "reporting_level"])[col].shift(1)

        # Create shift_year column
        tb["shift_year"] = tb.groupby(["country", "reporting_level"])["year"].shift(1)

        # Create shift_welfare_type column
        tb["shift_welfare_type"] = tb.groupby(["country", "reporting_level"])["welfare_type"].shift(1)

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
            if DEBUG:
                log.warning(
                    f"""There are {len(tb_error)} observations with abnormal jumps for {col}:
                    {tabulate(tb_error[['ppp_version', 'country', 'year', 'reporting_level', col, 'check_diff_column', 'check_diff_year']].sort_values('year').reset_index(drop=True), headers = 'keys', tablefmt = TABLEFMT, floatfmt=".1f")}"""
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

    return tb


def regional_headcount(tb: Table) -> Table:
    """
    Create regional headcount dataset, by patching missing values with the difference between world and regional headcount
    """

    # Keep only regional data: for regions, these are the reporting_level rows not in ['national', 'urban', 'rural']
    tb_regions = tb[~tb["reporting_level"].isin(["national", "urban", "rural"])].reset_index(drop=True)

    # Remove Western and Central and Eastern and Southern Africa. It's redundant with Sub-Saharan Africa (PIP)
    tb_regions = tb_regions[
        ~tb_regions["country"].isin(
            [
                "Western and Central Africa (PIP)",
                "Eastern and Southern Africa (PIP)",
                "World (excluding China)",
                "World (excluding India)",
            ]
        )
    ].reset_index(drop=True)

    # Select needed columns and pivot
    tb_regions = tb_regions[["country", "year", f"headcount_{INTERNATIONAL_POVERTY_LINE_CURRENT}"]]
    tb_regions = tb_regions.pivot(
        index="year", columns="country", values=f"headcount_{INTERNATIONAL_POVERTY_LINE_CURRENT}"
    ).reset_index()

    # Drop rows with more than one region with null headcount
    tb_regions["check_total"] = tb_regions[tb_regions.columns].isnull().sum(axis=1)
    mask = tb_regions["check_total"] > 1

    tb_out = tb_regions[mask].reset_index()
    if len(tb_out) > 0:
        log.info(
            f"""There are {len(tb_out)} years with more than one null region value so we can't extract regional data for them. Years are:
            {list(tb_out.year.unique())}"""
        )
        tb_regions = tb_regions[~mask].reset_index()

    tb_regions = tb_regions.drop(columns="check_total", errors="raise")

    # Get difference between world and (total) regional headcount, to patch rows with one missing value
    cols_to_sum = [e for e in list(tb_regions.columns) if e not in ["year", "World"]]
    tb_regions["sum_regions"] = tb_regions[cols_to_sum].sum(axis=1)

    tb_regions["diff_world_regions"] = tb_regions["World"] - tb_regions["sum_regions"]

    # Fill null values with the difference and drop aux variables
    col_dictionary = dict.fromkeys(cols_to_sum, tb_regions["diff_world_regions"])
    tb_regions.loc[:, cols_to_sum] = tb_regions[cols_to_sum].fillna(col_dictionary)
    tb_regions = tb_regions.drop(columns=["World", "sum_regions", "diff_world_regions"], errors="raise")

    # NOTE: I am not extracting data for China and India at least for now, because we are only extracting non filled data
    # The data originally came from filled data to plot properly.

    # # Get headcount values for China and India
    # df_chn_ind = tb[(tb["country"].isin(["China", "India"])) & (tb["reporting_level"] == "national")].reset_index(
    #     drop=True
    # )
    # df_chn_ind = df_chn_ind[["country", "year", "headcount_215"]]

    # # Make table wide and merge with regional data
    # df_chn_ind = df_chn_ind.pivot(index="year", columns="country", values="headcount_215").reset_index()
    # tb_regions = pr.merge(tb_regions, df_chn_ind, on="year", how="left")

    # tb_regions["East Asia and Pacific excluding China"] = (
    #     tb_regions["East Asia and Pacific (PIP)"] - tb_regions["China"]
    # )
    # tb_regions["South Asia excluding India"] = tb_regions["South Asia (PIP)"] - tb_regions["India"]

    tb_regions = pr.melt(
        tb_regions, id_vars=["year"], var_name="country", value_name=f"headcount_{INTERNATIONAL_POVERTY_LINE_CURRENT}"
    )
    tb_regions = tb_regions[["country", "year", f"headcount_{INTERNATIONAL_POVERTY_LINE_CURRENT}"]]

    # Rename headcount_{INTERNATIONAL_POVERTY_LINE_CURRENT} to headcount_{INTERNATIONAL_POVERTY_LINE_CURRENT}_regions, to distinguish it from the original headcount when merging
    tb_regions = tb_regions.rename(
        columns={
            f"headcount_{INTERNATIONAL_POVERTY_LINE_CURRENT}": f"headcount_{INTERNATIONAL_POVERTY_LINE_CURRENT}_regions"
        },
        errors="raise",
    )

    # Merge with original table
    tb = pr.merge(tb, tb_regions, on=["country", "year"], how="outer")

    return tb


def survey_count(tb: Table) -> Table:
    """
    Create survey count indicator, by counting the number of surveys available for each country in the past decade
    """
    # Remove regions from the table
    tb_survey = tb[~tb["country"].isin(REGIONS_LIST)].reset_index(drop=True)

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

    # Merge cross and df_country, to include all the possible rows in the dataset
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
    tb_survey["surveys_past_decade"] = tb_survey["surveys_past_decade"].copy_metadata(tb["reporting_level"])

    # Keep columns needed
    tb_survey = tb_survey[["country", "year", "surveys_past_decade"]]

    # Merge with original table
    tb = pr.merge(tb_survey, tb, on=["country", "year"], how="outer")

    return tb


def drop_columns(tb: Table) -> Table:
    """
    Drop columns not needed
    """

    # Remove columns
    tb = tb.drop(
        columns=[
            "ppp_version",
            "reporting_pop",
            "is_interpolated",
            "distribution_type",
            "estimation_type",
            "survey_comparability",
            "comparable_spell",
        ],
        errors="raise",
    )

    return tb


def create_survey_spells(tb: Table) -> list:
    """
    Create tables for each indicator and survey spells, to be able to graph them in explorers.
    """

    tb = tb.copy()

    # drop rows where survey coverage = nan (This is just regions)
    tb = tb[tb["survey_comparability"].notna()].reset_index()

    # Add 1 to make comparability var run from 1, not from 0
    tb["survey_comparability"] += 1

    # Note the welfare type in the comparability spell
    tb["survey_comparability"] = (
        tb["welfare_type"].astype(str) + "_spell_" + tb["survey_comparability"].astype(int).astype(str)
    )

    # Remove columns not needed: stacked, above, etc
    drop_list = ["above", "between", "poverty_severity", "watts"]
    for var in drop_list:
        tb = tb[tb.columns.drop(list(tb.filter(like=var)), errors="raise")]

    vars = [
        i
        for i in tb.columns
        if i
        not in [
            "country",
            "year",
            "ppp_version",
            "reporting_level",
            "welfare_type",
            "reporting_pop",
            "is_interpolated",
            "distribution_type",
            "estimation_type",
            "survey_comparability",
            "comparable_spell",
            f"headcount_{INTERNATIONAL_POVERTY_LINE_CURRENT}_regions",
            "surveys_past_decade",
        ]
    ]

    # Define spell table list
    spell_tables = []

    # Loop over the variables in the main dataset
    for select_var in vars:
        tb_var = tb[["country", "year", select_var, "survey_comparability"]].copy()

        # convert to wide
        tb_var = pr.pivot(
            tb_var,
            index=["country", "year"],
            columns=["survey_comparability"],
            values=select_var,
        )

        tb_var.metadata.short_name = f"{tb_var.metadata.short_name}_{select_var}"

        spell_tables.append(tb_var)

    return spell_tables


def create_survey_spells_inc_cons(tb_inc: Table, tb_cons: Table) -> list:
    """
    Create table for each indicator and survey spells, to be able to graph them in explorers.
    This version recombines income and consumption tables to not lose dropped rows.
    """

    tb_inc = tb_inc.reset_index()
    tb_cons = tb_cons.reset_index()

    # Concatenate the two tables
    tb_inc_or_cons_spells = pr.concat(
        [tb_inc, tb_cons], ignore_index=True, short_name=f"income_consumption_{PPP_YEAR_CURRENT}"
    )

    # Set index and sort
    tb_inc_or_cons_spells = tb_inc_or_cons_spells.format(keys=["country", "year", "reporting_level", "welfare_type"])

    # Create spells
    spell_tables = create_survey_spells(tb_inc_or_cons_spells)

    return spell_tables


def combine_tables_ppp_comparison(tb_ppp_old: Table, tb_ppp_current: Table, short_name: str) -> Table:
    """
    Combine income and consumption tables from PPP_YEAR_OLD and PPP_YEAR_CURRENT PPPs.
    We will use this table for the Poverty Data Explorer: World Bank data - PPP_YEAR_OLD vs. PPP_YEAR_CURRENT prices.
    """

    # Identify columns to use (ID + indicators)
    id_cols = ["country", "year"]

    tb_ppp_old = define_columns_for_ppp_comparison(tb=tb_ppp_old, id_cols=id_cols, ppp_version=PPP_YEAR_OLD)
    tb_ppp_current = define_columns_for_ppp_comparison(tb=tb_ppp_current, id_cols=id_cols, ppp_version=PPP_YEAR_CURRENT)

    # Rename all the non-id columns with the suffix _ppp(year)
    # (the suffix option in merge only adds suffix when columns coincide)
    tb_ppp_old = tb_ppp_old.rename(
        columns={c: c + f"_ppp{PPP_YEAR_OLD}" for c in tb_ppp_old.columns if c not in id_cols}, errors="raise"
    )
    tb_ppp_current = tb_ppp_current.rename(
        columns={c: c + f"_ppp{PPP_YEAR_CURRENT}" for c in tb_ppp_current.columns if c not in id_cols}, errors="raise"
    )

    # Merge the two files (it's OK to have an inner join, because we want to keep country-year pairs that are in both files)
    tb_ppp_comparison = pr.merge(tb_ppp_old, tb_ppp_current, on=id_cols, validate="one_to_one", short_name=short_name)

    # Add index and sort
    tb_ppp_comparison = tb_ppp_comparison.format(["country", "year"])

    return tb_ppp_comparison


def define_columns_for_ppp_comparison(tb: Table, id_cols: list, ppp_version: int) -> Table:
    """
    Define columns to use for the comparison of PPP_VERSIONS
    """

    tb = tb.reset_index()
    # Define poverty lines
    povlines_list = POVLINES_DICT[ppp_version]

    # Define groups of columns
    headcount_absolute_cols = [f"headcount_{p}" for p in povlines_list]
    headcount_ratio_absolute_cols = [f"headcount_ratio_{p}" for p in povlines_list]

    headcount_relative_cols = [f"headcount_{rel}_median" for rel in [40, 50, 60]]
    headcount_ratio_relative_cols = [f"headcount_ratio_{rel}_median" for rel in [40, 50, 60]]

    # Define all the columns to filter

    cols_list = (
        id_cols
        + headcount_absolute_cols
        + headcount_ratio_absolute_cols
        + headcount_relative_cols
        + headcount_ratio_relative_cols
        + ["mean", "median", "decile1_thr", "decile9_thr"]
    )

    # Filter columns
    tb = tb[cols_list]

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

    # Merge with the main table
    tb = pr.merge(
        tb, tb_percentiles_thr, on=["ppp_version", "country", "year", "reporting_level", "welfare_type"], how="left"
    )
    tb = pr.merge(
        tb, tb_percentiles_share, on=["ppp_version", "country", "year", "reporting_level", "welfare_type"], how="left"
    )

    # Now I can calculate the share of the top 90-99%
    tb["top90_99_share"] = tb["decile10_share"] - tb["top1_share"]

    return tb


def add_region_definitions(tb: Table, tb_region_definitions: Table) -> Table:
    """
    Add region definitions to the main table
    """

    tb = tb.copy()
    tb_region_definitions = tb_region_definitions.copy()

    # Merge with the main table
    tb = pr.merge(tb, tb_region_definitions, on=["country", "year"], how="outer")

    return tb

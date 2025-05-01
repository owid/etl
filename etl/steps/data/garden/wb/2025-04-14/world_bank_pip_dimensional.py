"""
Load a meadow dataset and create a garden dataset.

When running this step in an update, be sure to check all the outputs and logs to ensure the data is correct.

NOTE: To extract the log of the process (to review sanity checks, for example), run the following command in the terminal:
    nohup uv run etl run world_bank_pip > output.log 2>&1 &

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
# TODO: Modify the lines in 2021 prices
POVLINES_DICT = {
    2011: [100, 190, 320, 550, 1000, 2000, 3000, 4000],
    2017: [100, 215, 365, 685, 1000, 2000, 3000, 4000],
}

# Define PPP versions from POVLINES_DICT
PPP_VERSIONS = list(POVLINES_DICT.keys())

PPP_YEAR_OLD = PPP_VERSIONS[0]
PPP_YEAR_CURRENT = PPP_VERSIONS[1]

# Define current International Poverty Line (in cents)
# NOTE: Modify if poverty lines are updated from source
# TODO: Modify the lines in 2021 prices
INTERNATIONAL_POVERTY_LINE_CURRENT = 215

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
]

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
    tb = identify_rural_urban(tb)

    # Create stacked variables from headcount and headcount_ratio
    tb = create_stacked_variables(tb=tb)

    # NOTE: Uncomment this
    # # Sanity checks. I don't run for percentile tables because that process was done in the extraction
    # tb = sanity_checks(tb=tb)

    # Separate out consumption-only, income-only. Also, create a table with both income and consumption
    tb = inc_or_cons_data(tb)

    # Create regional headcount variable, by patching missing values with the difference between world and regional headcount
    tb = regional_headcount(tb)

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

    # #
    # # Save outputs.
    # #
    # # Create a new garden dataset with the same metadata as the meadow dataset.
    # ds_garden = paths.create_dataset(
    #     tables=tables,
    #     check_variables_metadata=True,
    #     default_metadata=ds_meadow.metadata,
    # )

    # # Save changes in the new garden dataset.
    # ds_garden.save()

    #
    # Process data.
    #

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def create_new_indicators_and_format(tb: Table) -> Table:
    """
    Create new indicators from the existing ones and format names and shares
    """
    # rename columns
    tb = tb.rename(columns={"headcount": "headcount_ratio", "poverty_gap": "poverty_gap_index"})

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
        ]
    )

    # Changing the decile(i) variables for decile(i)_share
    for i in range(1, 11):
        tb = tb.rename(columns={f"decile{i}": f"decile{i}_share"})

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
    tb_percentiles_thr = tb_percentiles_thr.rename(columns={"thr": "top1_thr"})

    tb_percentiles_share = tb_percentiles_share[
        ["ppp_version", "country", "year", "reporting_level", "welfare_type", "share", "avg"]
    ]
    tb_percentiles_share = tb_percentiles_share.rename(columns={"share": "top1_share", "avg": "top1_avg"})

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


def regional_data_from_1990(tb: Table, regions_list: list) -> Table:
    """
    Select regional data only from 1990 onwards, due to the uncertainty in 1980s data
    """
    # Create a regions table
    tb_regions = tb[(tb["year"] >= 1990) & (tb["country"].isin(regions_list))].reset_index(drop=True).copy()

    # Remove regions from tb
    tb = tb[~tb["country"].isin(regions_list)].reset_index(drop=True).copy()

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
    tb = tb.drop(columns=["reporting_level"])

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
            "ppp_version",
            "poverty_line",
            "headcount_ratio",
            "headcount",
            "reporting_pop",
        ]
    ].copy()

    # Pivot and obtain the poverty lines dictionary
    tb_pivot, povlines_dict = pivot_and_obtain_povlines_dict(
        tb=tb_pivot,
        index=["country", "year", "welfare_type"],
        columns=["ppp_version", "poverty_line"],
    )

    for ppp_year, povlines in povlines_dict.items():
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
    tb_pivot = tb_pivot.stack(future_stack=True)
    tb_pivot = tb_pivot.stack(future_stack=True).reset_index()

    # Merge with tb
    tb = pr.merge(
        tb,
        tb_pivot,
        on=["country", "year", "welfare_type", "poverty_line", "ppp_version"],
        how="outer",
    )

    return tb


def pivot_and_obtain_povlines_dict(tb: Table, index: List[str], columns: List[str]) -> Tuple[Table, dict]:
    """
    Pivot the table to calculate indicator more easily and create a dictionary with the ppp_version and their corresponding poverty_line
    """
    tb = tb.copy()

    # Pivot the table to calculate indicator more easily
    tb_pivot = tb.pivot(index=index, columns=columns)

    # Create a dictionary with the ppp_version and their corresponding poverty_line for headcount column, without repeating the values
    povlines_dict = {}
    for ppp_version in tb_pivot.columns.levels[1]:
        povlines_dict[ppp_version] = sorted(
            list(
                set(
                    [
                        col[1]
                        for col in tb_pivot.xs(ppp_version, level="ppp_version", axis=1).columns
                        if col[0] == "headcount"
                        and not tb_pivot.xs(ppp_version, level="ppp_version", axis=1)[col].isna().all()
                    ]
                )
            ),
            key=int,
        )

    print(povlines_dict)

    return tb_pivot, povlines_dict


def sanity_checks(
    tb: Table,
) -> Table:
    """
    Sanity checks for the table
    """

    # Define index for pivot
    index = ["country", "year", "welfare_type"]

    # Pivot and obtain the poverty lines dictionary
    tb_pivot, povlines_dict = pivot_and_obtain_povlines_dict(
        tb=tb, index=index, columns=["ppp_version", "poverty_line"]
    )

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

    for ppp_year, povlines in povlines_dict.items():
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
        for i in range(len(povlines)):
            # if it's the first value only continue
            if i == 0:
                continue

            # If it's the last value calculate the people between this value and the previous
            # and also the people over this poverty line (and percentages)
            else:
                varname_n = ("headcount_between", ppp_year, f"{povlines[i-1]} and {povlines[i]}")
                varname_pct = ("headcount_ratio_between", ppp_year, f"{povlines[i-1]} and {povlines[i]}")
                col_stacked_n_all.append(varname_n)
                col_stacked_pct_all.append(varname_pct)

        col_stacked_pct_all = (
            [("headcount_ratio", ppp_year, povlines[0])]
            + col_stacked_pct_all
            + [("headcount_ratio_above", ppp_year, povlines[-1])]
        )

        col_stacked_n_all = (
            [("headcount", ppp_year, povlines[0])] + col_stacked_n_all + [("headcount_above", ppp_year, povlines[-1])]
        )

        col_stacked_pct_dict[ppp_year] = {"all": col_stacked_pct_all}
        col_stacked_n_dict[ppp_year] = {"all": col_stacked_n_all}

        # Define the stacked columns for a reduced set of intervals
        col_stacked_pct_reduced = [
            ("headcount_ratio", ppp_year, povlines[1]),
            ("headcount_ratio_between", ppp_year, f"{povlines[1]} and {povlines[4]}"),
            ("headcount_ratio_between", ppp_year, f"{povlines[4]} and {povlines[6]}"),
            ("headcount_ratio_above", ppp_year, povlines[6]),
        ]
        col_stacked_n_reduced = [
            ("headcount", ppp_year, povlines[1]),
            ("headcount_between", ppp_year, f"{povlines[1]} and {povlines[4]}"),
            ("headcount_between", ppp_year, f"{povlines[4]} and {povlines[6]}"),
            ("headcount_above", ppp_year, povlines[6]),
        ]

        # Add the reduced columns to the dictionary
        col_stacked_pct_dict[ppp_year]["reduced"] = col_stacked_pct_reduced
        col_stacked_n_dict[ppp_year]["reduced"] = col_stacked_n_reduced

        # Calculate and check the sum of the stacked values
        tb_pivot["sum_pct"] = tb_pivot[col_stacked_pct_dict[ppp_year]["all"]].sum(axis=1)
        mask = (tb_pivot["sum_pct"] >= 100 + PRECISION_PERCENTAGE) | (tb_pivot["sum_pct"] <= 100 - PRECISION_PERCENTAGE)
        tb_error = tb_pivot[mask].reset_index(drop=True)

        if not tb_error.empty:
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
            log.warning(
                f"""There are {len(tb_error)} observations with missing poverty values and will be deleted:
                {tabulate(tb_error[(index + ["headcount_ratio"])], headers = 'keys', tablefmt = TABLEFMT)}"""
            )

            tb_pivot = tb_pivot[~mask].reset_index(drop=True)

        ############################
        # Missing median, mean and gini
        for indicator in ["mean", "median", "gini"]:
            # Check if the indicator is missing
            mask = tb_pivot[(indicator, ppp_year, povlines[1])].isna()
            tb_error = tb_pivot[mask].reset_index(drop=True)

            if not tb_error.empty:
                log.info(
                    f"""There are {len(tb_error)} observations with missing {indicator}. They will be not deleted."""
                )

        ############################
        # Missing decile shares
        # Define mask as all col_decile_share columns with ppp_year = ppp_year and povlines[1]
        cols_to_check = [(col, ppp_year, povlines[1]) for col in col_decile_share]
        mask = tb_pivot[cols_to_check].isna().any(axis=1)
        tb_error = tb_pivot[mask].reset_index(drop=True)
        if not tb_error.empty:
            log.info(
                f"""There are {len(tb_error)} observations with missing decile shares. They will be not deleted."""
            )

        ############################
        # Missing decile thresholds
        cols_to_check = [(col, ppp_year, povlines[1]) for col in col_decile_thr]
        mask = tb_pivot[cols_to_check].isna().any(axis=1)
        tb_error = tb_pivot[mask].reset_index(drop=True)
        if not tb_error.empty:
            log.info(
                f"""There are {len(tb_error)} observations with missing decile thresholds. They will be not deleted."""
            )

        ############################
        # Missing decile averages
        cols_to_check = [(col, ppp_year, povlines[1]) for col in col_decile_avg]
        mask = tb_pivot[cols_to_check].isna().any(axis=1)
        tb_error = tb_pivot[mask].reset_index(drop=True)
        if not tb_error.empty:
            log.info(
                f"""There are {len(tb_error)} observations with missing decile averages. They will be not deleted."""
            )

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
        tb_error = tb_pivot[mask].reset_index(drop=True).copy()

        if not tb_error.empty:
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
        tb_error = tb_pivot[mask].reset_index(drop=True).copy()

        if not tb_error.empty:
            log.warning(
                f"""{len(tb_error)} observations of shares (with top 1%) are not adding up to 100% and will be converted to null:
                {tabulate(tb_error[index + ['sum_pct']], headers = 'keys', tablefmt = TABLEFMT, floatfmt=".1f")}"""
            )
            # Make columns None if mask is True
            tb_pivot.loc[mask, [("top90_99_share", ppp_year, povlines[1]), ("top1_share", ppp_year, povlines[1])]] = (
                None
            )

        ############################
        # delete columns created for the checks
        tb_pivot = tb_pivot.drop(columns=m_check_vars + ["m_check_1", "check_total", "sum_pct"])

        obs_after_checks = len(tb_pivot)
        log.info(f"Sanity checks deleted {obs_before_checks - obs_after_checks} observations for {ppp_year} PPPs.")

    # Restore the format of the table
    tb = tb_pivot.set_index(index).stack(future_stack=True).reset_index()

    # Stack again to remove the level for ppp_version
    tb = tb.set_index(index + ["poverty_line"]).stack(future_stack=True).reset_index()

    return tb


def inc_or_cons_data(tb: Table) -> Tuple[Table, Table, Table, Table]:
    """
    Separate income and consumption data
    """

    # Make a copy of the table
    tb_spells = tb.copy()
    tb_no_spells = tb.copy()

    # Generate tb_inc_spells and tb_cons_spells
    tb_inc_spells = tb_spells[tb_spells["welfare_type"] == "income"].reset_index(drop=True)
    tb_cons_spells = tb_spells[tb_spells["welfare_type"] == "consumption"].reset_index(drop=True)

    # Drop the survey_comparability column for tb_no_spells
    tb_no_spells = tb_no_spells.drop(columns=["survey_comparability"])

    # Generate tb_inc_no_spells and tb_cons_no_spells
    tb_inc_no_spells = tb_no_spells[tb_no_spells["welfare_type"] == "income"].reset_index(drop=True)
    tb_cons_no_spells = tb_no_spells[tb_no_spells["welfare_type"] == "consumption"].reset_index(drop=True)

    # Create tb_no_spells_smooth, which cleans tb_no_spells, by removing jumps generated by changes in welfare_type
    tb_no_spells_smooth = create_smooth_inc_cons_series(tb_no_spells)

    # TODO: Come back to fix this
    # check_jumps_in_grapher_dataset(tb_no_spells_smooth)

    # Add the column table, identifying the type of table to use in Grapher
    tb_spells["table"] = "Income or consumption with spells"
    tb_inc_spells["table"] = "Income with spells"
    tb_cons_spells["table"] = "Consumption with spells"
    tb_no_spells["table"] = "Income or consumption"
    tb_inc_no_spells["table"] = "Income"
    tb_cons_no_spells["table"] = "Consumption"
    tb_no_spells_smooth["table"] = "Income or consumption consolidated"

    # Also, rename welfare_type to "Income or consumption" for tb_no_spells_smooth
    tb_no_spells_smooth["welfare_type"] = "Income or consumption"

    # Concatenate all these tables
    tb = pr.concat(
        [
            tb_spells,
            tb_inc_spells,
            tb_cons_spells,
            tb_no_spells,
            tb_inc_no_spells,
            tb_cons_no_spells,
            tb_no_spells_smooth,
        ],
        ignore_index=True,
    )

    return tb


def create_smooth_inc_cons_series(tb: Table) -> Table:
    """
    Construct an income and consumption series that is a combination of the two.
    """

    tb = tb.copy()

    # Pivot and obtain the poverty lines dictionary
    tb, povlines_dict = pivot_and_obtain_povlines_dict(
        tb=tb,
        index=["country", "year", "welfare_type"],
        columns=["ppp_version", "poverty_line"],
    )

    # Reset index in tb_both_inc_and_cons
    tb = tb.reset_index()

    # Sort values
    tb = tb.sort_values(by=["country", "year", "welfare_type"], ignore_index=True)

    # Flag duplicates per year – indicating multiple welfare_types
    tb["duplicate_flag"] = tb.duplicated(subset=[("country", "", ""), ("year", "", "")], keep=False)

    # Create a boolean column that is true if each country has only income or consumption
    tb["only_inc_or_cons"] = tb.groupby(["country"])["welfare_type"].transform(lambda x: x.nunique() == 1)

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

        # If there are only two welfare series, use both, except for countries where we have to choose one
        if number_of_welfare_series == 2:
            # assert if last_welfare type values are expected
            if country in ["Armenia", "Belarus", "Kyrgyzstan", "North Macedonia", "Peru"]:
                if country in ["Armenia", "Belarus", "Kyrgyzstan"]:
                    welfare_expected = ["consumption"]
                    assert len(last_welfare_type) == 1 and last_welfare_type == welfare_expected, log.fatal(
                        f"{country} has unexpected values of welfare_type: {last_welfare_type} instead of {welfare_expected}."
                    )

                elif country in ["North Macedonia", "Peru"]:
                    assert len(last_welfare_type) == 1 and last_welfare_type == ["income"], log.fatal(
                        f"{country} has unexpected values of welfare_type: {last_welfare_type} instead of ['income']"
                    )

                tb_country = tb_country[tb_country["welfare_type"].isin(last_welfare_type)].reset_index(drop=True)

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
        elif country in ["Haiti", "Philippines", "Romania", "Saint Lucia"]:
            welfare_expected = ["consumption", "income"]
            assert len(last_welfare_type) == 2 and last_welfare_type == welfare_expected, log.fatal(
                f"{country} has unexpected values of welfare_type: {last_welfare_type} instead of {welfare_expected}"
            )
            if country in ["Haiti", "Romania", "Saint Lucia"]:
                tb_country = tb_country[tb_country["welfare_type"] == "income"].reset_index(drop=True)
            else:
                tb_country = tb_country[tb_country["welfare_type"] == "consumption"].reset_index(drop=True)

        else:
            # Here I keep the most recent welfare type
            if country in ["Albania", "Ukraine"]:
                welfare_expected = ["consumption"]
                assert len(last_welfare_type) == 1 and last_welfare_type == welfare_expected, log.fatal(
                    f"{country} has unexpected values of welfare_type: {last_welfare_type} instead of {welfare_expected}."
                )
            else:
                welfare_expected = ["income"]
                assert len(last_welfare_type) == 1 and last_welfare_type == welfare_expected, log.fatal(
                    f"{country} has unexpected values of welfare_type: {last_welfare_type} instead of {welfare_expected}."
                )

            tb_country = tb_country[tb_country["welfare_type"].isin(last_welfare_type)].reset_index(drop=True)

        tb_both_inc_and_cons_smoothed = pr.concat([tb_both_inc_and_cons_smoothed, tb_country])

    # Restore the format of the table
    tb_both_inc_and_cons_smoothed = (
        tb_both_inc_and_cons_smoothed.set_index(
            ["country", "year", "welfare_type"]
        )  # Set the desired index, including the additional columns
        .stack(level=["ppp_version", "poverty_line"], future_stack=True)  # Stack the MultiIndex columns
        .reset_index()  # Reset the index to flatten the table
    )

    # Do the same with tb_only_inc_or_cons
    tb_only_inc_or_cons = (
        tb_only_inc_or_cons.set_index(["country", "year", "welfare_type"])
        .stack(level=["ppp_version", "poverty_line"], future_stack=True)
        .reset_index()
    )

    tb_inc_or_cons = pr.concat([tb_only_inc_or_cons, tb_both_inc_and_cons_smoothed], ignore_index=True)

    # Drop the columns created in this function
    tb_inc_or_cons = tb_inc_or_cons.drop(columns=["only_inc_or_cons", "duplicate_flag"])

    return tb_inc_or_cons


def check_jumps_in_grapher_dataset(tb: Table) -> None:
    """
    Check for jumps in the dataset, which can be caused by combining income and consumption estimates for one country series.
    """
    tb = tb.copy()

    # Pivot and obtain the poverty lines dictionary
    tb, povlines_dict = pivot_and_obtain_povlines_dict(
        tb=tb,
        index=["country", "year", "welfare_type"],
        columns=["ppp_version", "poverty_line"],
    )

    # Reset index in tb
    tb = tb.reset_index()

    # For each country, year, welfare_type and reporting_level, check if the difference between the columns is too high

    for ppp_year, povlines in povlines_dict.items():
        # Define columns to check: all the headcount ratio columns
        cols_to_check = [("headcount_ratio", ppp_year, povline) for povline in povlines]
        for col in cols_to_check:
            print(f"Checking {col}")
            # Create a new column, shift_col, that is the same as col but shifted one row down for each country
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
                (abs(tb["check_diff_column"].fillna(0)) > 10)
                & (tb["check_diff_year"].fillna(0) <= 5)
                & (~tb["check_diff_welfare_type"].fillna(False))
            )
            tb_error = tb[mask].reset_index(drop=True)

            if not tb_error.empty:
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
        ]
    )

    return None


def regional_headcount(tb: Table) -> Table:
    """
    Create regional headcount dataset, by patching missing values with the difference between world and regional headcount
    """

    # From REGIONS_LIST,, drop the regions we are not interested in
    regions_for_headcount = [
        regions
        for regions in REGIONS_LIST
        if regions
        not in [
            "Western and Central Africa (PIP)",
            "Eastern and Southern Africa (PIP)",
            "World (excluding China)",
            "World (excluding India)",
        ]
    ]

    # Keep only regional data
    tb_regions = tb[tb["country"].isin(regions_for_headcount)].reset_index(drop=True)

    # Keep only the data for the table "Income or consumption consolidated"
    tb_regions = tb_regions[tb_regions["table"] == "Income or consumption consolidated"].reset_index(drop=True)

    # Select needed columns and pivot
    tb_regions = tb_regions[["country", "year", "ppp_version", "poverty_line", "table", "headcount"]]

    # Pivot and obtain the poverty lines dictionary
    tb_regions_aux, povlines_dict = pivot_and_obtain_povlines_dict(
        tb=tb_regions,
        index=["country", "year", "table"],
        columns=["ppp_version", "poverty_line"],
    )

    # From povlines_dict, get the [1]th value for each ppp_year
    ipl_list = [povlines_dict[ppp_year][1] for ppp_year in povlines_dict.keys()]

    # Filter the table to keep only the rows with the poverty line we are interested in
    tb_regions = tb_regions[tb_regions["poverty_line"].isin(ipl_list)].reset_index(drop=True)

    print(tb_regions)

    # Pivot the table to have one column per region
    tb_regions = tb_regions.pivot(
        index=["ppp_version", "poverty_line", "year"], columns="country", values="headcount"
    ).reset_index()

    print(tb_regions)

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

    tb_regions = tb_regions.drop(columns="check_total")

    # Get difference between world and (total) regional headcount, to patch rows with one missing value
    cols_to_sum = [e for e in list(tb_regions.columns) if e not in ["year", "World"]]
    tb_regions["sum_regions"] = tb_regions[cols_to_sum].sum(axis=1)

    tb_regions["diff_world_regions"] = tb_regions["World"] - tb_regions["sum_regions"]

    # Fill null values with the difference and drop aux variables
    col_dictionary = dict.fromkeys(cols_to_sum, tb_regions["diff_world_regions"])
    tb_regions.loc[:, cols_to_sum] = tb_regions[cols_to_sum].fillna(col_dictionary)
    tb_regions = tb_regions.drop(columns=["World", "sum_regions", "diff_world_regions"])

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
        }
    )

    # Merge with original table
    tb = pr.merge(tb, tb_regions, on=["country", "year"], how="outer")

    return tb


def survey_count(tb: Table) -> Table:
    """
    Create survey count indicator, by counting the number of surveys available for each country in the past decade
    """
    # Remove regions from the table
    tb_survey = tb[~tb["country"].isin(REGIONS_LIST)].reset_index(drop=True).copy()

    min_year = int(tb_survey["year"].min())
    max_year = int(tb_survey["year"].max())
    year_list = list(range(min_year, max_year + 1))
    country_list = list(tb_survey["country"].unique())

    # Create two tables with all the years and entities
    year_tb_survey = Table(year_list)
    entity_tb_survey = Table(country_list)

    # Make a cartesian product of both dataframes: join all the combinations between all the entities and all the years
    cross = pr.merge(entity_tb_survey, year_tb_survey, how="cross")
    cross = cross.rename(columns={"0_x": "country", "0_y": "year"})

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
        ]
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
        tb = tb[tb.columns.drop(list(tb.filter(like=var)))]

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

    tb_inc = tb_inc.reset_index().copy()
    tb_cons = tb_cons.reset_index().copy()

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
        columns={c: c + f"_ppp{PPP_YEAR_OLD}" for c in tb_ppp_old.columns if c not in id_cols}
    )
    tb_ppp_current = tb_ppp_current.rename(
        columns={c: c + f"_ppp{PPP_YEAR_CURRENT}" for c in tb_ppp_current.columns if c not in id_cols}
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


def add_region_definitions(tb: Table, tb_region_definitions: Table) -> Table:
    """
    Add region definitions to the main table
    """

    tb = tb.copy()
    tb_region_definitions = tb_region_definitions.copy()

    # Merge with the main table
    tb = pr.merge(tb, tb_region_definitions, on=["country", "year"], how="outer")

    return tb


def show_not_dimensional_data_once(tb: Table) -> Table:
    """
    Make all the columns that are not dimensional (do not depend on dimensions) to be shown once.
    """

    return tb


def make_shares_and_thresholds_long(tb: Table) -> Table:
    """
    Convert decile1, ..., decile10 and decile1_thr, ..., decile9_thr to a long format.
    """
    tb = tb.copy()

    # Define index columns
    index_columns = ["country", "year", "reporting_level", "welfare_type", "ppp_version"]

    # Define share columns
    share_columns = [f"decile{i}" for i in range(1, 11)]
    tb_share = tb.melt(
        id_vars=index_columns,
        value_vars=share_columns,
        var_name="decile",
        value_name="share",
    )

    # Add an empty poverty_line column
    tb_share["poverty_line"] = None

    # Define threshold columns
    thr_columns = [f"decile{i}_thr" for i in range(1, 10)]
    tb_thr = tb.melt(
        id_vars=index_columns,
        value_vars=thr_columns,
        var_name="decile",
        value_name="thr",
    )

    # Add an empty poverty_line column
    tb_thr["poverty_line"] = None

    # Create an empty decile column in tb
    tb["decile"] = None

    # Merge tb and tb_share
    tb = pr.merge(tb, tb_share, on=index_columns + ["decile", "poverty_line"], how="outer")

    # Merge tb and tb_thr
    tb = pr.merge(tb, tb_thr, on=index_columns + ["decile", "poverty_line"], how="outer")

    # Remove share_columns and threshold_columns
    tb = tb.drop(columns=share_columns + thr_columns)

    # Remove "decile" from the decile column
    tb["decile"] = tb["decile"].str.replace("decile", "")

    # Do the same with "_thr"
    tb["decile"] = tb["decile"].str.replace("_thr", "")

    return tb
    return tb

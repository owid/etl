"""
This code is used to select observations from PIP, WID or LIS datasets that match a pair of reference years.
It selects the closest observation to the reference year, and in the case of PIP, trying to ensure that is, in this order:
    1. The same welfare concept (first income, then consumption)
    2. The same reporting level (first national, then urban, then rural)

This is an adaptation of the original script created by Pablo A and Joe for Joe's PhD project, available at https://github.com/owid/notebooks/blob/main/JoeHasell/PhD_2024/paper2/select_and_prepare_observations.py
We want to process this data inside the ETL now.
"""

from typing import Dict, List

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger
log = get_logger()

# Specify pairs of reference years in a list
REF_YRS_LIST = [[1980, 2018], [1993, 2018]]

REFERENCE_YEARS = {
    1980: {"maximum_distance": 5, "tie_break_strategy": "lower", "min_interval": 0},
    2018: {"maximum_distance": 5, "tie_break_strategy": "higher", "min_interval": 0},
}


def run(dest_dir: str) -> None:
    # Load dataset and table
    ds_pov_ineq = paths.load_dataset("poverty_inequality_file")
    ds_population = paths.load_dataset("population")
    ds_regions = paths.load_dataset("regions")

    tb = ds_pov_ineq.read("keyvars")

    # Change types of some columns to avoid issues with filering and missing values on merge
    tb = tb.astype({"pipreportinglevel": "object", "pipwelfare": "object", "series_code": "object"})

    tb_population = ds_population.read("population")
    tb_population = tb_population[["country", "year", "population"]]

    #### SET REF YEARS AND THEN RUN ####

    for ref_yrs in REF_YRS_LIST:
        reference_years = {
            ref_yrs[0]: {"maximum_distance": 5, "tie_break_strategy": "lower", "min_interval": 0},
            ref_yrs[1]: {"maximum_distance": 5, "tie_break_strategy": "higher", "min_interval": 0},
        }

        # Version 1 – All data points (for both WID extrap and non-extrap series)
        # (NB only_all_series = False)
        tb_main = match_ref_years(
            tb=tb,
            series=[
                "gini_pip_disposable_perCapita",
                # "p99p100Share_pip_disposable_perCapita",
                # "p90p100Share_pip_disposable_perCapita",
                # "headcountRatio50Median_pip_disposable_perCapita",
                # "gini_widExtrapolated_pretaxNational_perAdult",
                # "p99p100Share_widExtrapolated_pretaxNational_perAdult",
                # "p90p100Share_widExtrapolated_pretaxNational_perAdult",
                # "headcountRatio50Median_widExtrapolated_pretaxNational_perAdult",
                # "gini_wid_pretaxNational_perAdult",
                # "p99p100Share_wid_pretaxNational_perAdult",
                # "p90p100Share_wid_pretaxNational_perAdult",
                # "headcountRatio50Median_wid_pretaxNational_perAdult",
            ],
            reference_years=reference_years,
            only_all_series=False,
            tb_population=tb_population,
            ds_regions=ds_regions,
        )

        # Make a version for OWID plot
        # Reshape from wide to long format
        owid_data = pd.wide_to_long(
            tb_main, ["value", "year", "population"], i=["country", "series_code"], j="ref_year", sep="_"
        ).reset_index()

        # Pivot from long to wide format, creating a column for each series_code
        owid_data = owid_data.pivot_table(
            index=["country", "year"], columns="series_code", values="value", aggfunc="first"
        ).reset_index()

        # Save a version for the appendix table – excluding WID extrapolated data

        # Reshape wider

        # Drop extrapolated WID series

        # Drop rows where series_code includes 'Extrapolated'
        tb_main = tb_main[~tb_main["series_code"].str.contains("widExtrapolated", na=False)]

        # Replace series_code with 'pip' or 'wid'
        tb_main["series_code"] = tb_main["series_code"].apply(lambda x: "pip" if "pip" in x else "wid")

        # Identify the columns that need to be reshaped
        varying_columns = [
            f"year_{ref_yrs[0]}",
            f"value_{ref_yrs[0]}",
            f"year_{ref_yrs[1]}",
            f"value_{ref_yrs[1]}",
            f"pipwelfare_{ref_yrs[0]}",
            f"pipwelfare_{ref_yrs[1]}",
            f"pipreportinglevel_{ref_yrs[0]}",
            f"pipreportinglevel_{ref_yrs[1]}",
        ]
        non_varying_columns = ["region", f"population_{ref_yrs[0]}", f"population_{ref_yrs[1]}"]

        # Use pivot to reshape the data
        tb_main_wide = tb_main.pivot_table(
            index=["indicator_name", "country"], columns="series_code", values=varying_columns, aggfunc="first"
        ).reset_index()

        # Flatten the MultiIndex in columns
        tb_main_wide.columns = ["_".join(col).strip() if col[1] else col[0] for col in tb_main_wide.columns.values]

        # Merge the non-varying columns back to the reshaped dataframe
        tb_main_non_varying = tb_main.drop_duplicates(subset=["indicator_name", "country"])[
            non_varying_columns + ["indicator_name", "country"]
        ]

        tb_main_reshaped = pd.merge(tb_main_non_varying, tb_main_wide, on=["indicator_name", "country"])

        # Sort rows
        tb_main_reshaped = tb_main_reshaped.sort_values(by=["indicator_name", "country"], ascending=[True, True])

        # I then make a wider dataset that matches PIP and WID datapoints
        # – first excluding extrapolated WID data (which should match the above), then adding any additional extrapolated data points in

        # Step 1 – Only matching data points, non-extrapolated data for WID
        # (NB only_all_series = True)
        # output_wid_non_extrap = match_ref_years(
        #     tb=tb,
        #     series=[
        #         "gini_pip_disposable_perCapita",
        #         "p99p100Share_pip_disposable_perCapita",
        #         "p90p100Share_pip_disposable_perCapita",
        #         "headcountRatio50Median_pip_disposable_perCapita",
        #         "gini_wid_pretaxNational_perAdult",
        #         "p99p100Share_wid_pretaxNational_perAdult",
        #         "p90p100Share_wid_pretaxNational_perAdult",
        #         "headcountRatio50Median_wid_pretaxNational_perAdult",
        #     ],
        #     reference_years=reference_years,
        #     only_all_series=True,
        #     tb_population=tb_population,
        #     ds_regions=ds_regions,
        # )

        # Step 2 Only matching data points, with extrapolated data for WID
        # (NB only_all_series = True)
        # output_wid_extrap = match_ref_years(
        #     tb=tb,
        #     series=[
        #         "gini_pip_disposable_perCapita",
        #         "p99p100Share_pip_disposable_perCapita",
        #         "p90p100Share_pip_disposable_perCapita",
        #         "headcountRatio50Median_pip_disposable_perCapita",
        #         "gini_widExtrapolated_pretaxNational_perAdult",
        #         "p99p100Share_widExtrapolated_pretaxNational_perAdult",
        #         "p90p100Share_widExtrapolated_pretaxNational_perAdult",
        #         "headcountRatio50Median_widExtrapolated_pretaxNational_perAdult",
        #     ],
        #     reference_years=reference_years,
        #     only_all_series=True,
        #     tb_population=tb_population,
        #     ds_regions=ds_regions,
        # )

        #
        output_wid_extrap["series_code"] = output_wid_extrap["series_code"].str.replace("Extrapolated", "", regex=False)

        # Step 3 – add in the any additional observations available in the extrapolated data to the non-extrpolated data

        # Drop the population and region cols from the non_extrap data perpared above (aso that we can recalculate the pop weights based on the new sample)
        # Find columns in tb2 that are not in tb1
        columns_to_drop = output_wid_non_extrap.columns.difference(output_wid_extrap.columns)

        #
        # Drop these columns from tb2
        output_wid_non_extrap = output_wid_non_extrap.drop(columns=columns_to_drop)

        #
        # Append the extrap and non-extrap outputs, adding a key
        output_matched = pd.concat(
            [output_wid_non_extrap, output_wid_extrap], axis=0, keys=["wid_not_extrapolated", "wid_extrapolated"]
        )

        #

        # Reset the index to make the keys a separate column
        output_matched.reset_index(level=0, inplace=True)

        # Rename the 'level_0' column to 'key'
        output_matched.rename(columns={"level_0": "key"}, inplace=True)

        # Reset the index to make it sequential
        output_matched.reset_index(drop=True, inplace=True)

        #
        # count number of observations by series_code and country
        columns_to_consider = ["series_code", "country"]
        output_matched["count"] = output_matched.groupby(columns_to_consider)[columns_to_consider[0]].transform("count")

        #
        # Drop the extrpolated data if the count is 2 (i.e. if there is an observation from the non extrap data)
        output_matched = output_matched[
            (output_matched["count"] == 1) | (output_matched["key"] == "wid_not_extrapolated")
        ]

        # Drop the count column
        output_matched = output_matched.drop(columns=["count"])

        # Reshape wider
        #
        # Replace series_code with 'pip' or 'wid'
        output_matched["series_code"] = output_matched["series_code"].apply(lambda x: "pip" if "pip" in x else "wid")

        # Identify the columns that need to be reshaped
        # varying_columns are as above
        non_varying_columns = (
            ["key"] + non_varying_columns
        )  # non_varying_columns now has a 'key' column reporting whether the wid data is from the extrapolated data matches or not

        # Use pivot to reshape the data
        output_matched_wide = output_matched.pivot_table(
            index=["indicator_name", "country"], columns="series_code", values=varying_columns, aggfunc="first"
        ).reset_index()

        # Flatten the MultiIndex in columns
        output_matched_wide.columns = [
            "_".join(col).strip() if col[1] else col[0] for col in output_matched_wide.columns.values
        ]

        # Merge the non-varying columns back to the reshaped dataframe
        output_matched_non_varying = output_matched.drop_duplicates(subset=["indicator_name", "country"])[
            non_varying_columns + ["indicator_name", "country"]
        ]

        output_matched_reshaped = pd.merge(
            output_matched_non_varying, output_matched_wide, on=["indicator_name", "country"]
        )

        # Sort rows
        output_matched_reshaped = output_matched_reshaped.sort_values(
            by=["indicator_name", "country"], ascending=[True, True]
        )

        # I then append the 'main' and 'matched' sets of observtions, deduplicating
        output_all_obs = pd.concat([tb_main_reshaped, output_matched_reshaped], axis=0)
        output_all_obs = output_all_obs.sort_values(by=["indicator_name", "country"], ascending=[True, True])

        #
        # count number of observations by series_code and country
        columns_to_consider = ["indicator_name", "country"]
        output_all_obs["count"] = output_all_obs.groupby(columns_to_consider)[columns_to_consider[0]].transform("count")

        #
        # Keep the matched data (in which case, key is not NaN) if the count is 2 (the matched data contains additional observations)
        output_all_obs = output_all_obs[(output_all_obs["count"] == 1) | (output_all_obs["key"].notna())]

        output_all_obs_test = output_all_obs.copy()

        # Apply the function to each row to create the new column
        output_all_obs["wid_is_extrapolated"] = output_all_obs.apply(determine_wid_is_extrapolated, axis=1)

        #
        # Drop the count and key columns
        output_all_obs = output_all_obs.drop(columns=["count", "key"])

        # reorder columns slightly

        # Sort columns
        desired_column_order = [
            "indicator_name",
            "country",
            f"year_{ref_yrs[0]}_pip",
            f"value_{ref_yrs[0]}_pip",
            f"pipwelfare_{ref_yrs[0]}_pip",
            f"year_{ref_yrs[1]}_pip",
            f"value_{ref_yrs[1]}_pip",
            f"pipwelfare_{ref_yrs[1]}_pip",
            f"year_{ref_yrs[0]}_wid",
            f"value_{ref_yrs[0]}_wid",
            f"year_{ref_yrs[1]}_wid",
            f"value_{ref_yrs[1]}_wid",
            "wid_is_extrapolated",
            "region",
            f"population_{ref_yrs[0]}",
            f"population_{ref_yrs[1]}",
            f"pipreportinglevel_{ref_yrs[0]}_pip",
            f"pipreportinglevel_{ref_yrs[1]}_pip",
        ]

        # Reorder the DataFrame columns
        output_all_obs = output_all_obs.reindex(columns=desired_column_order)


##############################################


def match_ref_years(
    tb: Table, series: list, reference_years: dict, only_all_series: bool, tb_population: Table, ds_regions: Dataset
) -> pd.DataFrame:
    """
    Match series to reference years.
    This is the main function that finds pairs of matching observations
    In the case of PIP data, it calls the special functions above to handle the additional dimensions of that dataset (region, welfare measure)
    """

    tb_match = pd.DataFrame()
    tb_series = tb[tb["series_code"].isin(series)].reset_index(drop=True)

    reference_years_list = []
    for y in reference_years:
        # Keep reference year in a list
        reference_years_list.append(y)

        # Filter tb_series according to reference year and maximum distance from it
        tb_year = tb_series[
            (tb_series["year"] <= y + reference_years[y]["maximum_distance"])
            & (tb_series["year"] >= y - reference_years[y]["maximum_distance"])
        ].reset_index(drop=True)

        assert not tb_year.empty, log.error(
            f"No data found for reference year {y}. Please check `maximum_distance` ({reference_years[y]['maximum_distance']})."
        )

        # Calculate the distance between the observation year and the reference year (absolute value)
        tb_year["distance"] = abs(tb_year["year"] - y)

        # Merge the different reference year tables into a single dataframe

        if tb_match.empty:
            tb_match = tb_year
        else:
            tb_match = pr.merge(
                tb_match,
                tb_year,
                how="outer",
                on=["country", "series_code"],
                suffixes=("", f"_{y}"),
            )

            # References to column names work differently depending on if there are 2 or more reference years. Treat these cases separately.
            if len(reference_years_list) == 2:
                # Categorize the pipwelfare match
                tb_match["pipwelfarecat"] = tb_match.apply(cat_welfare, args=("pipwelfare", f"pipwelfare_{y}"), axis=1)

                # Categorize the pipreportinglevel match
                tb_match["pipreportinglevelcat"] = tb_match.apply(
                    cat_reportinglevel, args=("pipreportinglevel", f"pipreportinglevel_{y}"), axis=1
                )

                # Add a column that gives the distance between the observation years
                tb_match[f"distance_{reference_years_list[-2]}_{y}"] = abs(tb_match["year"] - tb_match[f"year_{y}"])

            else:
                # Categorize the pipwelfare match
                tb_match["pipwelfarecat"] = tb_match.apply(
                    cat_welfare, args=(f"pipwelfare_{reference_years_list[-2]}", f"pipwelfare_{y}"), axis=1
                )

                # Categorise the pipreportinglevel match
                tb_match["pipreportinglevelcat"] = tb_match.apply(
                    cat_reportinglevel,
                    args=(f"pipreportinglevel_{reference_years_list[-2]}", f"pipreportinglevel_{y}"),
                    axis=1,
                )

                # Add a column that gives the distance between the observation years
                tb_match[f"distance_{reference_years_list[-2]}_{y}"] = abs(
                    tb_match[f"year_{reference_years_list[-2]}"] - tb_match[f"year_{y}"]
                )

            # Filter tb_match according to best pipwelfarecat
            min_values = tb_match.groupby(["country", "series_code"])["pipwelfarecat"].transform("min")
            tb_match = tb_match[tb_match["pipwelfarecat"] == min_values]

            # Filter tb_match according to best pipreportinglevelcat
            min_values = tb_match.groupby(["country", "series_code"])["pipreportinglevelcat"].transform("min")
            tb_match = tb_match[tb_match["pipreportinglevelcat"] == min_values]

            # Filter tb_match according to min_interval
            tb_match = tb_match[
                tb_match[f"distance_{reference_years_list[-2]}_{y}"]
                >= reference_years[reference_years_list[-2]]["min_interval"]
            ].reset_index(drop=True)

            assert not tb_match.empty, log.error(
                f"No matching data found for reference years {reference_years_list[-2]} and {y}. Please check `min_interval` ({reference_years[reference_years_list[-2]]['min_interval']})."
            )

    # Rename columns related to the first reference year
    tb_match = tb_match.rename(
        columns={
            "year": f"year_{reference_years_list[0]}",
            "distance": f"distance_{reference_years_list[0]}",
            "value": f"value_{reference_years_list[0]}",
            "pipwelfare": f"pipwelfare_{reference_years_list[0]}",
            "pipreportinglevel": f"pipreportinglevel_{reference_years_list[0]}",
        }
    )

    # Filter tb_match according to tie_break_strategy
    for y in reference_years_list:
        # Calculate the minimum of distance for each country-series_code
        min_per_group = tb_match.groupby(["country", "series_code"])[f"distance_{y}"].transform("min")

        # Keep only the rows where distance is equal to the group minimum
        tb_match = tb_match[tb_match[f"distance_{y}"] == min_per_group]

        # count how many different years got matched to the reference year
        tb_match["unique_years_count"] = tb_match.groupby(["country", "series_code"])[f"year_{y}"].transform("nunique")

        if reference_years[y]["tie_break_strategy"] == "lower":
            # drop observations where the year is above the reference year, when there is more than one year that has been matched
            tb_match = tb_match[(tb_match["unique_years_count"] == 1) | (tb_match[f"year_{y}"] < y)]

        elif reference_years[y]["tie_break_strategy"] == "higher":
            # drop observations where the year is below the reference year, when there is more than one year that has been matched
            tb_match = tb_match[(tb_match["unique_years_count"] == 1) | (tb_match[f"year_{y}"] > y)]
        else:
            raise ValueError("tie_break_strategy must be either 'lower' or 'higher'")

        assert not tb_match.empty, log.error(
            f"No matching data data found for reference year {y}. Please check `tie_break_strategy` ({reference_years[y]['tie_break_strategy']})."
        )

    # Create a list with the variables year_y and value_y for each reference year
    year_y_list = []
    value_y_list = []
    year_value_y_list = []
    pipwelfare_y_list = []
    pipreportinglevel_y_list = []

    for y in reference_years_list:
        year_y_list.append(f"year_{y}")
        value_y_list.append(f"value_{y}")
        year_value_y_list.append(f"year_{y}")
        year_value_y_list.append(f"value_{y}")
        pipwelfare_y_list.append(f"pipwelfare_{y}")
        pipreportinglevel_y_list.append(f"pipreportinglevel_{y}")

    # Make columns in year_y_list integer
    tb_match[year_y_list] = tb_match[year_y_list].astype(int)

    # Keep the columns I need
    tb_match = tb_match[
        ["country", "series_code", "indicator_name"] + year_value_y_list + pipwelfare_y_list + pipreportinglevel_y_list
    ].reset_index(drop=True)

    # Sort by country and year_y
    tb_match = tb_match.sort_values(by=["series_code", "country"] + year_y_list).reset_index(drop=True)

    # If set in the function arguments, filter for only those countries
    #  avaiable in all series.
    if only_all_series:
        # Identify countries present for every unique series_code
        countries_per_series_code = tb_match.groupby("series_code")["country"].unique()
        all_countries = set(tb_match["country"])

        # Find countries that are present in every series_code
        countries_in_all_series = set(countries_per_series_code.iloc[0])
        for countries in countries_per_series_code:
            countries_in_all_series &= set(countries)

        # Filter the dataframe to keep only rows where country is in the identified set
        tb_match = tb_match[tb_match["country"].isin(countries_in_all_series)]
    # Add regions
    tb_match = add_regions_columns(tb=tb_match, ds_regions=ds_regions)

    # Add population

    for y in reference_years_list:
        tb_pop_year = tb_population[tb_population["year"] == y].copy().reset_index(drop=True)
        tb_pop_year = tb_pop_year.drop(columns="year")

        tb_match = pr.merge(tb_match, tb_pop_year, how="left")

        tb_match = tb_match.rename(columns={"population": f"population_{y}"})

    return tb_match


#  PIP DATA SELECTION FUNCTIONS
# The PIP data has reporting level (national, urban, rural) and welfare type (income or consumption).
# Sometimes, taking observations closest to the reference years may result in non-matching data points in these two dimensions.
# These two functions are called within the main function below so as to prioritize matches with consistent definitions.abs


def cat_welfare(row, col1, col2):
    """
    'Scores' PIP data pairs of years as to their welfare concept. A pair of income observations is best, a pair of consumption observations is second best and non-matching welfare is ranked third.
    """
    if row[col1] == "income" and row[col2] == "income":
        return 1
    elif row[col1] == "consumption" and row[col2] == "consumption":
        return 2
    else:
        return 3


def cat_reportinglevel(row, col1, col2):
    """
    'Scores' PIP data pairs of years as to their 'reporting_level' (urban, rural, or national).
    A pair of national observations is best, a pair of urban observations is second best, a pair of rural observations is third best, and non-matching observations is ranked fourth.
    """
    if row[col1] == "national" and row[col2] == "national":
        return 1
    elif row[col1] == "urban" and row[col2] == "urban":
        return 2
    elif row[col1] == "rural" and row[col2] == "rural":
        return 3
    else:
        return 4


# Note WID extrapolated or not in a boolean
# Define the function to determine the value of 'wid_is_extrapolated'
def determine_wid_is_extrapolated(row, ref_yrs):
    if pd.isna(row[f"value_{ref_yrs[0]}_wid"]):
        return np.nan
    return 1 if row["key"] == "wid_extrapolated" else 0


def add_regions_columns(tb: Table, ds_regions: Dataset) -> Table:
    """
    Add region columns to the table.
    """

    tb_regions = geo.create_table_of_regions_and_subregions(ds_regions=ds_regions)

    # Explode the regions table to have one row per country
    tb_regions = tb_regions.explode("members").reset_index(drop=False)

    # Select OWID regions
    tb_regions = tb_regions[
        tb_regions["region"].isin(["North America", "South America", "Europe", "Africa", "Asia", "Oceania"])
    ].reset_index(drop=True)

    # Merge the regions table with the table
    tb = pr.merge(
        tb,
        tb_regions,
        left_on="country",
        right_on="members",
        how="left",
    )

    # Delete the members column
    tb = tb.drop(columns=["members"])

    # Keep only the rows where region is not missing
    tb = tb.dropna(subset=["region"]).reset_index(drop=True)

    return tb

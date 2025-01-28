"""
This code is used to select observations from PIP, WID or LIS datasets that match a pair of reference years.
It selects the closest observation to the reference year, and in the case of PIP, trying to ensure that is, in this order:
    1. The same welfare concept (first income, then consumption)
    2. The same reporting level (first national, then urban, then rural)

This is an adaptation of the original script created by Pablo A and Joe for Joe's PhD project, available at https://github.com/owid/notebooks/blob/main/JoeHasell/PhD_2024/paper2/select_and_prepare_observations.py
We want to process this data inside the ETL now.
"""

from typing import Dict, List

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

# Define columns that we want to analyze
INDICATORS_FOR_ANALYSIS = {
    "gini_pip_disposable_perCapita": "gini",
    "p90p100Share_pip_disposable_perCapita": "decile10_share",
    "gini_wid_pretaxNational_perAdult": "p0p100_gini_pretax",
    "p99p100Share_wid_pretaxNational_perAdult": "p99p100_share_pretax",
    "p90p100Share_wid_pretaxNational_perAdult": "p90p100_share_pretax",
    "gini_wid_posttaxNational_perAdult": "p0p100_gini_posttax_nat",
    "p99p100Share_wid_posttaxNational_perAdult": "p99p100_share_posttax_nat",
    "p90p100Share_wid_posttaxNational_perAdult": "p90p100_share_posttax_nat",
}


# Define reference years and parameters for matching
# maximum_distance: maximum distance from the reference year that an observation can be
# tie_break_strategy: how to break ties when there are multiple observations at the same distance from the reference year
# min_interval: minimum distance between the observation year and the reference year
REFERENCE_YEARS = [
    {
        1980: {"maximum_distance": 5, "tie_break_strategy": "lower", "min_interval": 0},
        2018: {"maximum_distance": 5, "tie_break_strategy": "higher", "min_interval": 0},
    },
    {
        1993: {"maximum_distance": 5, "tie_break_strategy": "lower", "min_interval": 0},
        2018: {"maximum_distance": 5, "tie_break_strategy": "higher", "min_interval": 0},
    },
    {
        1980: {"maximum_distance": 5, "tie_break_strategy": "lower", "min_interval": 0},
        2019: {"maximum_distance": 5, "tie_break_strategy": "higher", "min_interval": 0},
    },
    {
        1993: {"maximum_distance": 5, "tie_break_strategy": "lower", "min_interval": 0},
        2019: {"maximum_distance": 5, "tie_break_strategy": "higher", "min_interval": 0},
    },
    {
        1980: {"maximum_distance": 5, "tie_break_strategy": "lower", "min_interval": 0},
        2020: {"maximum_distance": 2, "tie_break_strategy": "higher", "min_interval": 0},
    },
    {
        1993: {"maximum_distance": 5, "tie_break_strategy": "lower", "min_interval": 0},
        2020: {"maximum_distance": 2, "tie_break_strategy": "higher", "min_interval": 0},
    },
    {
        1980: {"maximum_distance": 5, "tie_break_strategy": "lower", "min_interval": 0},
        2023: {"maximum_distance": 2, "tie_break_strategy": "higher", "min_interval": 0},
    },
    {
        1993: {"maximum_distance": 5, "tie_break_strategy": "lower", "min_interval": 0},
        2023: {"maximum_distance": 2, "tie_break_strategy": "higher", "min_interval": 0},
    },
]


def run(dest_dir: str) -> None:
    # Load dataset and table
    ds_pov_ineq = paths.load_dataset("poverty_inequality_file")
    ds_population = paths.load_dataset("population")
    # ds_regions = paths.load_dataset("regions")
    ds_pip = paths.load_dataset("world_bank_pip")
    ds_wid = paths.load_dataset("world_inequality_database")
    ds_lis = paths.load_dataset("luxembourg_income_study")

    tb = ds_pov_ineq.read("keyvars")

    # Load tables from PIP, WID, and LIS datasets (for metadata)
    tb_pip = ds_pip["income_consumption_2017_unsmoothed"].reset_index()
    tb_wid = ds_wid["world_inequality_database"].reset_index()
    tb_lis = ds_lis["luxembourg_income_study"].reset_index()

    # Change types of some columns to avoid issues with filering and missing values on merge
    tb = tb.astype({"pipreportinglevel": "object", "pipwelfare": "object", "series_code": "object"})

    #### SET REF YEARS AND THEN RUN ####
    # Define an empty list of tables
    tables = []

    for reference_years in REFERENCE_YEARS:
        # Version 1 – All data points (only_all_series = False)
        tb_all_data_points = match_ref_years(
            tb=tb,
            series=INDICATORS_FOR_ANALYSIS.keys(),
            reference_years=reference_years,
            only_all_series=False,
        )

        # Append the table to the list
        tables.append(tb_all_data_points)

        # Version 2 - Only countries with data in all series (only_all_series = True)
        tb_data_in_all_series = match_ref_years(
            tb=tb,
            series=INDICATORS_FOR_ANALYSIS.keys(),
            reference_years=reference_years,
            only_all_series=True,
        )

        # Append the table to the list
        tables.append(tb_data_in_all_series)

    # Concatenate tables
    tb = pr.concat(tables, ignore_index=True)

    # Add metadata from original tables
    tb = add_metadata_from_original_tables(
        tb=tb, indicator_match=INDICATORS_FOR_ANALYSIS, tb_pip=tb_pip, tb_wid=tb_wid, tb_lis=tb_lis
    )

    # # Add regions
    # tb = add_regions_columns(tb=tb, ds_regions=ds_regions)

    # Add population
    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population, year_col="ref_year")

    # Format the table
    tb = tb.format(
        keys=["country", "year", "ref_year", "year_1", "year_2", "only_all_series"], short_name="inequality_comparison"
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_pov_ineq.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


##############################################


def match_ref_years(
    tb: Table,
    series: List[str],
    reference_years: Dict[int, Dict[str, int]],
    only_all_series: bool,
) -> Table:
    """
    Match series to reference years.
    This is the main function that finds pairs of matching observations
    In the case of PIP data, it calls the special functions above to handle the additional dimensions of that dataset (region, welfare measure)
    """

    tb_match = Table(pd.DataFrame())
    tb_series = tb[tb["series_code"].isin(series)].copy().reset_index(drop=True)

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

                # Categorize the pipreportinglevel match
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
        tb_match["min_per_group"] = tb_match.groupby(["country", "series_code"])[f"distance_{y}"].transform("min")

        # Keep only the rows where distance is equal to the group minimum
        tb_match = tb_match[tb_match[f"distance_{y}"] == tb_match["min_per_group"]].reset_index(drop=True)

        # count how many different years got matched to the reference year
        tb_match["unique_years_count"] = tb_match.groupby(["country", "series_code"])[f"year_{y}"].transform("nunique")

        if reference_years[y]["tie_break_strategy"] == "lower":
            # drop observations where the year is above the reference year, when there is more than one year that has been matched
            tb_match = tb_match[(tb_match["unique_years_count"] == 1) | (tb_match[f"year_{y}"] < y)].reset_index(
                drop=True
            )

        elif reference_years[y]["tie_break_strategy"] == "higher":
            # drop observations where the year is below the reference year, when there is more than one year that has been matched
            tb_match = tb_match[(tb_match["unique_years_count"] == 1) | (tb_match[f"year_{y}"] > y)].reset_index(
                drop=True
            )
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

    # If set in the function arguments, filter for only those countries available in all series.
    if only_all_series:
        # Identify countries present for every unique series_code
        countries_per_series_code = tb_match.groupby("series_code")["country"].unique()

        # Find countries that are present in every series_code
        countries_in_all_series = set(countries_per_series_code.iloc[0])
        for countries in countries_per_series_code:
            countries_in_all_series &= set(countries)

        # Filter the dataframe to keep only rows where country is in the identified set
        tb_match = tb_match[tb_match["country"].isin(countries_in_all_series)].reset_index(drop=True)

    # Reshape from wide to long format
    tb_match = pd.wide_to_long(
        tb_match, ["value", "year"], i=["country", "series_code"], j="ref_year", sep="_"
    ).reset_index(drop=False)

    # Pivot from long to wide format, creating a column for each series_code
    tb_match = tb_match.pivot(
        index=["country", "year", "ref_year"],
        columns="series_code",
        values="value",
        join_column_levels_with="_",
    ).reset_index(drop=True)

    # Add dimensional identifiers for match
    tb_match["year_1"] = reference_years_list[0]
    tb_match["year_2"] = reference_years_list[1]
    tb_match["only_all_series"] = only_all_series

    # Replace only_all_series with a more descriptive name
    tb_match["only_all_series"] = tb_match["only_all_series"].replace({True: "Only countries with data in all series"})
    tb_match["only_all_series"] = tb_match["only_all_series"].replace({False: "All data points for each series"})

    return tb_match


#  PIP DATA SELECTION FUNCTIONS
# The PIP data has reporting level (national, urban, rural) and welfare type (income or consumption).
# Sometimes, taking observations closest to the reference years may result in non-matching data points in these two dimensions.
# These two functions are called within the main function below so as to prioritize matches with consistent definitions.abs


def cat_welfare(row, col1, col2):
    """
    'Scores' PIP data pairs of years as to their welfare concept. A pair of income observations is best, a pair of consumption observations is second best and non-matching welfare is ranked third.
    """
    if pd.isna(row[col1]) or pd.isna(row[col2]):
        return 3
    elif row[col1] == "income" and row[col2] == "income":
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
    if pd.isna(row[col1]) or pd.isna(row[col2]):
        return 4
    elif row[col1] == "national" and row[col2] == "national":
        return 1
    elif row[col1] == "urban" and row[col2] == "urban":
        return 2
    elif row[col1] == "rural" and row[col2] == "rural":
        return 3
    else:
        return 4


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


def add_metadata_from_original_tables(
    tb: Table, indicator_match: Dict[str, str], tb_pip: Table, tb_wid: Table, tb_lis: Table
) -> Table:
    """
    Add the original metadata we have in the garden steps of the main metadata.
    This way we can add origins and indicator-based metadata
    """

    for col, match in indicator_match.items():
        # If col contains "pip"
        if "pip" in col:
            # Get the metadata from the PIP table
            tb[col] = tb[col].copy_metadata(tb_pip[match])
        # If col contains "wid"
        elif "wid" in col:
            # Get the metadata from the WID table
            tb[col] = tb[col].copy_metadata(tb_wid[match])
        # If col contains "lis"
        elif "lis" in col:
            # Get the metadata from the LIS table
            tb[col] = tb[col].copy_metadata(tb_lis[match])

    return tb

"""Load a snapshot and create a meadow dataset."""

from typing import Dict

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()

# Define national strategies for homelessness
NATIONAL_STRATEGIES_YEAR = 2023
NATIONAL_STRATEGIES = {
    "Have an active national strategy": [
        "Belgium",
        "Canada",
        "Chile",
        "Colombia",
        "Costa Rica",
        "Czechia",
        "Denmark",
        "Finland",
        "France",
        "Germany",
        "Greece",
        "Ireland",
        "Italy",
        "Japan",
        "Korea",
        "Netherlands",
        "New Zealand",
        "Norway",
        "Poland",
        "Portugal",
        "Romania",
        "Slovak Republic",
        "Spain",
        "Sweden",
        "United States",
    ],
    "Have regional or local strategies": ["Australia", "Austria", "Estonia", "Iceland", "United Kingdom"],
    "No strategy": [
        "Bulgaria",
        "Croatia",
        "Cyprus",
        "Hungary",
        "Israel",
        "Latvia",
        "Lithuania",
        "Luxembourg",
        "Malta",
        "Mexico",
        "Slovenia",
        "Switzerland",
        "Türkiye",
    ],
}


# Column names and their new names.
COLUMNS_POINT_IN_TIME = {
    "Unnamed: 11": "country",
    "Unnamed: 12": "point_in_time_1",
    "Unnamed: 13": "point_in_time_2_3",
    "Unnamed: 14": "point_in_time_1_2_3",
    "Total": "point_in_time_total",
}

COLUMNS_FLOW = {
    "Unnamed: 18": "country",
    "Unnamed: 19": "flow_1",
    "Unnamed: 20": "flow_2_3",
    "Unnamed: 21": "flow_1_2_3",
    "Total.1": "flow_total",
}

COLUMNS_WOMEN = {
    "Unnamed: 17": "country",
    "Share of Women": "share_of_women",
}

COLUMNS_INDEX = {
    "Unnamed: 11": "country",
    "year": "year",
    "index": "index",
}

COLUMNS_SHARE = {
    "Unnamed: 16": "country",
    "year": "year",
    "share": "share",
}

COLUMNS_NUMBER = {
    "Country": "country",
    "year": "year",
    "number": "number",
}

# Define name of the first unnecessary row in the table number of people experiencing homelessness
NUMBER_FIRST_UNNECESSARY_ROW = ".. Not available, | break in series"

# Define the list of countries for each level of homelessness in the table of share of women experiencing homelessness

WOMEN_TYPE_HOMELESSNESS = {
    "1_2_3": [
        "U.K.(England)*",
        "New Zealand",
        "Australia",
        "Iceland",
        "France",
        "Slovak Republic",
        "United States",
        "Sweden",
        "Germany",
        "Canada",
        "Estonia",
        "Austria",
        "Norway",
        "Korea",
        "Portugal",
        "Denmark",
        "Spain",
        "Finland",
        "Netherlands",
        "Chile",
        "Poland",
        "Israel",
        "Costa Rica",
        "Colombia",
    ],
    "2_3": ["Ireland", "Italy", "Slovenia", "Greece", "Lithuania", "Croatia", "Latvia"],
    "1": ["Mexico", "Japan"],
}

# Define the list of countries with flow data in the share of women experiencing homelessness table
COUNTRIES_FLOW = ["Austria", "Chile", "Israel", "Italy", "Slovenia", "Lithuania", "Croatia", "Latvia"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("affordable_housing_database_homelessness.xlsx")

    # Load data from snapshot.
    # Point-in-time data
    tb_point_in_time = snap.read(sheet_name="HC3.1.1", usecols="L:P", skiprows=2, na_values=[".."])

    # Flow data
    tb_flow = snap.read(sheet_name="HC3.1.1", usecols="S:W", skiprows=2, na_values=[".."])

    # Share of women
    tb_women = snap.read(sheet_name="HC3.1.2", usecols="R:S", skiprows=1, na_values=[".."])

    # Index of people experiencing homelessness
    tb_index = snap.read(sheet_name="HC3.1.3", usecols="L:S", skiprows=6, na_values=[".."])

    # Share trends
    tb_share = snap.read(sheet_name="HC3.1.4", usecols="Q:AE", skiprows=6, na_values=[".."])

    # Number of people experiencing homelessness
    tb_number = snap.read(sheet_name="Table_HC3.1.A2", skiprows=3, na_values=[".."])

    #
    # Process data.
    tb_point_in_time = rename_columns_drop_rows_and_format(
        tb_point_in_time, columns=COLUMNS_POINT_IN_TIME, short_name="point_in_time", year=False
    )
    tb_flow = rename_columns_drop_rows_and_format(tb_flow, columns=COLUMNS_FLOW, short_name="flow", year=False)
    tb_women = rename_columns_drop_rows_and_format(
        tb_women, columns=COLUMNS_WOMEN, short_name="share_of_women", year=False
    )
    tb_women = add_count_and_type_of_homelessness_to_women_data(tb_women)

    # Make tb_index long first
    tb_index = tb_index.dropna(how="all", ignore_index=True).melt(
        id_vars=["Unnamed: 11"], var_name="year", value_name="index"
    )
    tb_index = rename_columns_drop_rows_and_format(tb_index, columns=COLUMNS_INDEX, short_name="index", year=True)

    # Make tb_trends long first
    tb_share = tb_share.dropna(how="all", ignore_index=True).melt(
        id_vars=["Unnamed: 16"], var_name="year", value_name="share"
    )
    tb_share = rename_columns_drop_rows_and_format(tb_share, columns=COLUMNS_SHARE, short_name="share", year=True)

    # Apply more processing to tb_number
    tb_number = remove_unnecesary_rows(tb_number)
    tb_number = clean_numbers_and_make_long(tb_number)
    tb_number = rename_columns_drop_rows_and_format(tb_number, columns=COLUMNS_NUMBER, short_name="number", year=True)

    # Add national strategies to the table
    tb_national_strategies = add_national_strategies(NATIONAL_STRATEGIES, tb_number)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[
            tb_point_in_time,
            tb_flow,
            tb_women,
            tb_index,
            tb_share,
            tb_number,
            tb_national_strategies,
        ],
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def rename_columns_drop_rows_and_format(tb: Table, columns: Dict[str, str], short_name: str, year: bool) -> Table:
    """
    Rename columns, drop nan rows, and format the table.
    """

    # Assert if columns are in the table
    assert all(col in tb.columns for col in columns.keys()), log.fatal("Columns not found in the table.")

    # Rename columns
    tb = tb.rename(columns=columns, errors="raise")

    # Drop nan rows
    tb = tb.dropna(how="all", ignore_index=True)

    # Drop nan country rows (there are hidden rows in HC3.1.1 sheet)
    tb = tb.dropna(subset=["country"], how="all", ignore_index=True)

    # Remove trailing spaces in the country column
    tb["country"] = tb["country"].str.strip()

    # Format
    if year:
        # Remove nulls in the short_name column, the main indicator column
        tb = tb.dropna(subset=short_name, how="all", ignore_index=True)
        tb = tb.format(["country", "year"], short_name=short_name)
    else:
        tb = tb.format(["country"], short_name=short_name)

    return tb


def remove_unnecesary_rows(tb: Table) -> Table:
    """
    Remove rows that are not needed.
    """

    # Drop nan rows in Country
    tb = tb.dropna(subset=["Country"], how="all", ignore_index=True)

    # Assert if NUMBER_FIRST_UNNECESSARY_ROW exist in the country column
    assert tb["Country"].str.contains(NUMBER_FIRST_UNNECESSARY_ROW).any(), log.fatal(
        log.fatal(
            f"Message '{NUMBER_FIRST_UNNECESSARY_ROW}' not found in the table number of people experiencing homelessness."
        )
    )

    # Get the row index of the first occurrence of NUMBER_FIRST_UNNECESSARY_ROW and drop all rows after that
    index = tb[tb["Country"].str.contains(NUMBER_FIRST_UNNECESSARY_ROW)].index[0]
    tb = tb.drop(tb.index[index:])

    return tb


def clean_numbers_and_make_long(tb: Table) -> Table:
    """
    Clean the spaces between numbers, remove "\n(households)" and make the table long.
    """

    # Remove countries containing "United Kingdom" (they use households instead of people)
    tb = tb[~tb["Country"].str.contains("United Kingdom")]

    # Remove spaces between numbers and force the ones with commas to be numeric
    for col in tb.columns:
        if col == "Country":
            continue
        else:
            tb[col] = tb[col].replace(r"\s+", "", regex=True)
            tb[col] = tb[col].apply(pd.to_numeric, errors="coerce")

    # Make the table long
    tb = tb.melt(id_vars=["Country"], var_name="year", value_name="number")

    # Replace year column with the 4-digit year contained in the column
    tb["year"] = tb["year"].str.extract(r"(\d{4})").astype(int)

    return tb


def add_count_and_type_of_homelessness_to_women_data(tb: Table) -> Table:
    """
    Add count and type of homelessness to the table with the share of women experiencing homelessness.
    """

    tb = tb.reset_index().copy()

    # Make a list of all the countries listed for level of homelessness
    all_countries = [country for countries in WOMEN_TYPE_HOMELESSNESS.values() for country in countries]

    # Assert if all all_countries are in the table
    assert all(country in tb["country"].unique() for country in all_countries), log.fatal(
        f"Countries defined for type of homelessness not found in the table: {[country for country in all_countries if country not in tb['country'].unique()]}"
    )

    # Assign level of homelessness to countries
    for type_homelessness, countries in WOMEN_TYPE_HOMELESSNESS.items():
        tb.loc[tb["country"].isin(countries), "type_homelessness"] = type_homelessness

    # Assign method of homelessness to countries
    tb["count_homelessness"] = "point_in_time"
    tb.loc[tb["country"].isin(COUNTRIES_FLOW), "count_homelessness"] = "flow"

    # Add metadata
    tb["type_homelessness"] = tb["type_homelessness"].copy_metadata(tb["country"])
    tb["count_homelessness"] = tb["count_homelessness"].copy_metadata(tb["country"])

    # Make table wide again using count_homelessness as a column
    tb = tb.pivot(index=["country"], columns=["count_homelessness", "type_homelessness"], join_column_levels_with="_")

    # Format
    tb = tb.format(["country"])

    return tb


def add_national_strategies(national_strategies: Dict[str, list], tb: Table) -> Table:
    """
    Create a table with national strategies for homelessness.
    """
    # Create table with national strategies
    tb_national_strategies = Table(
        pd.DataFrame(national_strategies.items(), columns=["national_strategy", "country"]),
        short_name="national_strategies",
    )

    # Explode country (each country has a row for each national strategy)
    tb_national_strategies = tb_national_strategies.explode("country")

    # Assign year to all rows
    tb_national_strategies["year"] = NATIONAL_STRATEGIES_YEAR

    # Add metadata from any column in tb (in this case number)
    for col in tb_national_strategies.columns:
        tb_national_strategies[col] = tb_national_strategies[col].copy_metadata(tb["number"])

    # Format
    tb_national_strategies = tb_national_strategies.format(["country", "year"])

    return tb_national_strategies

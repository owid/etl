"""Load a meadow dataset and create a garden dataset."""

from typing import Dict, List

import owid.catalog.processing as pr
from owid.catalog import Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()

# Define default year for the tables
DEFAULT_YEAR = 2023

# Define new years for the tables, representing the years for which the data is shown.
NEW_YEARS_PIT_FLOW = {
    2022: [
        "Austria",
        "Belgium",
        "Canada",
        "Czechia",
        "Denmark",
        "France",
        "Germany",
        "Latvia",
        "Lithuania",
        "Luxembourg",
        "Portugal",
        "Slovenia",
        "South Korea",
        "Spain",
        "Turkey",
    ],
    2021: ["Australia", "Croatia", "Iceland", "Italy"],
    2020: ["Mexico", "Norway"],
    2019: ["Poland"],
    2018: ["New Zealand"],
    2017: ["Cyprus", "Sweden"],
}

NEW_YEARS_WOMEN = {
    2022: [
        "United Kingdom",
        "Austria",
        "Canada",
        "Denmark",
        "Germany",
        "South Korea",
        "Lithuania",
        "Portugal",
        "Slovenia",
        "Spain",
    ],
    2021: ["Australia", "Croatia", "Iceland", "Italy", "Latvia"],
    2020: ["Mexico", "Norway"],
    2019: ["Poland"],
    2018: ["New Zealand"],
    2017: ["Sweden"],
}

NEW_YEARS_NUMBER = {
    2023: {
        2022: [
            "Austria",
            "Denmark",
            "Finland",
            "South Korea",
            "Latvia",
            "Lithuania",
            "Luxembourg",
            "Portugal",
            "Slovenia",
            "Spain",
        ],
        2021: ["Australia", "Colombia", "Estonia", "Iceland", "Israel", "Italy", "Slovakia"],
        2020: ["Norway"],
    },
    2018: {2019: ["Chile", "Poland"], 2017: ["Denmark", "Sweden"], 2016: ["Australia", "Norway"]},
    2015: {2013: ["Denmark", "Poland", "Spain"], 2014: ["Germany", "Luxembourg"], 2016: ["Australia", "Norway"]},
    2010: {
        2011: ["Australia", "Chile", "Denmark", "Estonia", "Latvia", "Slovakia", "Sweden"],
        2012: ["France", "Norway"],
    },
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("affordable_housing_database")

    # Read table from meadow dataset.
    tb_point_in_time = ds_meadow["point_in_time"].reset_index()
    tb_flow = ds_meadow["flow"].reset_index()
    tb_women = ds_meadow["share_of_women"].reset_index()
    tb_share = ds_meadow["share"].reset_index()
    tb_number = ds_meadow["number"].reset_index()
    tb_national_strategies = ds_meadow["national_strategies"].reset_index()

    #
    # Process data.
    # Harmonize each table before any processing, because the entities vary between sheets
    tb_point_in_time = geo.harmonize_countries(
        df=tb_point_in_time, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    tb_flow = geo.harmonize_countries(
        df=tb_flow, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    tb_women = geo.harmonize_countries(
        df=tb_women, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    tb_share = geo.harmonize_countries(
        df=tb_share, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    tb_number = geo.harmonize_countries(
        df=tb_number, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    tb_national_strategies = geo.harmonize_countries(
        df=tb_national_strategies, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )

    # Merge point_in_time and flow tables.
    tb = pr.merge(tb_point_in_time, tb_flow, on=["country"], how="outer", short_name=paths.short_name)

    # Add years to the table
    tb = add_years_to_table(tb, NEW_YEARS_PIT_FLOW)

    # Multiply all the columns except for country and year by 10
    for col in tb.columns:
        if col not in ["country", "year"]:
            tb[col] *= 10

    # Add years to women table
    tb_women = add_years_to_table(tb_women, NEW_YEARS_WOMEN)

    # Multiple all the columns except for country and year by 100 (percentage)
    for col in tb_women.columns:
        if col not in ["country", "year"]:
            tb_women[col] *= 100

    # Merge women table with main table
    tb = pr.merge(tb, tb_women, on=["country", "year"], how="outer", short_name=paths.short_name)

    # Merge share table with main table
    tb = pr.merge(tb, tb_share, on=["country", "year"], how="outer", short_name=paths.short_name)

    # Add years to number table
    tb_number = add_years_to_table_number_table(tb_number, NEW_YEARS_NUMBER)

    # Merge number table with main table
    tb = pr.merge(tb, tb_number, on=["country", "year"], how="outer", short_name=paths.short_name)

    # Merge national_strategies table with main table
    tb = pr.merge(tb, tb_national_strategies, on=["country", "year"], how="outer", short_name=paths.short_name)

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_years_to_table(tb: Table, years: Dict[int, List[str]]) -> Table:
    """Add years to the table. Though the data is shown for DEFAULT_YEAR, in the metadata it is mentioned that several countries have data from previous years."""
    tb = tb.copy()

    # Assign DEFAULT_YEAR to all rows
    tb["year"] = DEFAULT_YEAR

    # Make a list of all the countries listed in the dictionary
    all_countries = [country for countries in years.values() for country in countries]

    # Assert if all all_countries are in the table
    assert all(country in tb["country"].unique() for country in all_countries), log.fatal(
        f"Countries not found in the table: {[country for country in all_countries if country not in tb['country'].unique()]}"
    )

    # Assign specific years to countries
    for year, countries in years.items():
        tb.loc[tb["country"].isin(countries), "year"] = year

    return tb


def add_years_to_table_number_table(tb: Table, years: Dict[int, Dict[int, List[str]]]) -> Table:
    """Add years to the table number experiencing homelessness. Though the data is shown for some years, in the metadata it is mentioned that those years represent different years depending on the country."""
    tb = tb.copy()

    # Iterate over each of the original year columns
    for original_year in years.keys():
        # Make a list of all the countries listed in the dictionary
        all_countries = [country for countries in years[original_year].values() for country in countries]

        # Assert if all all_countries are in the table
        assert all(country in tb["country"].unique() for country in all_countries), log.fatal(
            f"Countries not found in the table: {[country for country in all_countries if country not in tb['country'].unique()]}"
        )

        # Assign specific years to countries
        for year, countries in years[original_year].items():
            tb.loc[(tb["year"] == original_year) & (tb["country"].isin(countries)), "year"] = year

    return tb

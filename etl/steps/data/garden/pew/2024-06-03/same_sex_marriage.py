"""Load a meadow dataset and create a garden dataset."""

from typing import List

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define last year (last update May 2023)
LATEST_YEAR = 2023

# Define regions to aggregate
REGIONS = ["Europe", "Asia", "North America", "South America", "Africa", "Oceania", "World"]

# Define fraction of allowed NaNs per year
FRAC_ALLOWED_NANS_PER_YEAR = 0.2


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset, regions and population
    ds_meadow = paths.load_dataset("same_sex_marriage")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["same_sex_marriage"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    tb = explode_country_years(tb=tb)

    tb = add_country_counts_and_population_by_status(
        tb=tb,
        columns=["legal_status"],
        ds_regions=ds_regions,
        ds_population=ds_population,
        regions=REGIONS,
        missing_data_on_columns=False,
    )

    # Redefine legal_status_not_legal_pop as legal_status_not_legal_pop + legal_status_missing_pop
    tb["legal_status_Not legal_pop"] = tb["legal_status_Not legal_pop"] + tb["legal_status_missing_pop"]

    # Drop legal_status_missing_pop and legal_status_not_legal_count
    tb = tb.drop(columns=["legal_status_missing_pop", "legal_status_Not legal_count"])

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


def explode_country_years(tb: Table) -> Table:
    """
    Create a row for each country-year from the minimum year to LATEST_YEAR
    """

    # Add category to define legal status
    tb["legal_status"] = "Legal"
    tb["legal_status"] = tb["legal_status"].copy_metadata(tb["country"])

    min_year = tb["year"].min() - 1

    # Define the list of countries
    countries = tb["country"].unique().tolist()

    # Create a table of years and another of countries
    tb_years = Table({"year": range(min_year, LATEST_YEAR + 1)})
    tb_countries = Table({"country": countries})

    # Create a new table with the cartesian product of tb_years and tb_countries
    tb_countries_years = pr.merge(tb_years, tb_countries, how="cross")

    # Merge this table with tb
    tb = pr.merge(tb_countries_years, tb, how="left", on=["country", "year"], short_name=paths.short_name)

    # Fill data forward after the first incidence of "Legal" in each country
    tb["legal_status"] = tb.groupby("country")["legal_status"].ffill()

    # Add "Not legal" for missing legal status
    tb.loc[tb["legal_status"].isnull(), "legal_status"] = "Not legal"

    return tb


def add_country_counts_and_population_by_status(
    tb: Table,
    columns: List[str],
    ds_regions: Dataset,
    ds_population: Dataset,
    regions: List[str],
    missing_data_on_columns: bool = False,
) -> Table:
    """
    Add country counts and population by status for the columns in the list
    """

    tb_regions = tb.copy()

    tb_regions = geo.add_population_to_table(
        tb=tb_regions, ds_population=ds_population, warn_on_missing_countries=False
    )

    # Define empty dictionaries for each of the columns
    columns_count_dict = {columns[i]: [] for i in range(len(columns))}
    columns_pop_dict = {columns[i]: [] for i in range(len(columns))}
    for col in columns:
        if missing_data_on_columns:
            # Fill nan values with "missing"
            tb_regions[col] = tb_regions[col].fillna("missing")
        # Get the unique values in the column
        status_list = list(tb_regions[col].unique())
        for status in status_list:
            # Calculate count and population for each status in the column
            tb_regions[f"{col}_{status}_count"] = tb_regions[col].apply(lambda x: 1 if x == status else 0)
            tb_regions[f"{col}_{status}_pop"] = tb_regions[f"{col}_{status}_count"] * tb_regions["population"]

            # Add the new columns to the list
            columns_count_dict[col].append(f"{col}_{status}_count")
            columns_pop_dict[col].append(f"{col}_{status}_pop")

    # Create a new list with all the count columns and population columns
    columns_count = [item for sublist in columns_count_dict.values() for item in sublist]
    columns_pop = [item for sublist in columns_pop_dict.values() for item in sublist]

    aggregations = dict.fromkeys(
        columns_count + columns_pop + ["population"],
        "sum",
    )

    tb_regions = geo.add_regions_to_table(
        tb=tb_regions,
        ds_regions=ds_regions,
        regions=regions,
        aggregations=aggregations,
        frac_allowed_nans_per_year=FRAC_ALLOWED_NANS_PER_YEAR,
    )

    # Remove population column
    tb_regions = tb_regions.drop(columns=["population"])

    # Add population again
    tb_regions = geo.add_population_to_table(
        tb=tb_regions, ds_population=ds_population, warn_on_missing_countries=False
    )

    # Calculate the missing population for each region
    for col in columns:
        # Calculate the missing population for each column, by subtracting the population of the countries with data from the total population
        tb_regions[f"{col}_missing_pop_other_countries"] = tb_regions["population"] - tb_regions[
            columns_pop_dict[col]
        ].sum(axis=1)
        if missing_data_on_columns:
            tb_regions[f"{col}_missing_pop"] = (
                tb_regions[f"{col}_missing_pop"] + tb_regions[f"{col}_missing_pop_other_countries"]
            )

        else:
            # Rename column
            tb_regions = tb_regions.rename(columns={f"{col}_missing_pop_other_countries": f"{col}_missing_pop"})

            # Append this missing population column to the list of population columns
            columns_pop.append(f"{col}_missing_pop")

    # Keep only the regions in the country column
    tb_regions = tb_regions[tb_regions["country"].isin(REGIONS)].copy().reset_index(drop=True)

    # Keep only the columns I need
    tb_regions = tb_regions[["country", "year"] + columns_count + columns_pop]

    # Merge the two tables
    tb = pr.merge(tb, tb_regions, on=["country", "year"], how="outer")

    return tb

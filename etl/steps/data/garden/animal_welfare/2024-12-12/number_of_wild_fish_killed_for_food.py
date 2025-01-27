"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Column names to select from the table on global numbers, and how to rename them.
COLUMNS_GLOBAL = {
    "year": "year",
    "estimated_numbers_in_millions__lower": "n_wild_fish_low",
    "estimated_numbers_in_millions__midpoint": "n_wild_fish",
    "estimated_numbers_in_millions__upper": "n_wild_fish_high",
}

# Column names to select from the table on country-level numbers, and how to rename them.
COLUMNS_BY_COUNTRY = {
    "country": "country",
    "estimated_numbers__lower": "n_wild_fish_low",
    "estimated_numbers__upper": "n_wild_fish_high",
    "estimated_numbers__midpoint": "n_wild_fish",
}


def add_per_capita_variables(tb: Table, ds_population: Dataset) -> Table:
    tb = geo.add_population_to_table(tb, ds_population=ds_population, warn_on_missing_countries=False)
    tb["n_wild_fish_low_per_capita"] = tb["n_wild_fish_low"] / tb["population"]
    tb["n_wild_fish_per_capita"] = tb["n_wild_fish"] / tb["population"]
    tb["n_wild_fish_high_per_capita"] = tb["n_wild_fish_high"] / tb["population"]
    # Drop population column.
    tb = tb.drop(columns=["population"], errors="raise")
    return tb


def run_sanity_checks_on_outputs(tb: Table) -> None:
    error = "Expected lower bound <= midpoint <= upper bound."
    assert (tb["n_wild_fish_low"] <= tb["n_wild_fish"]).all(), error
    assert (tb["n_wild_fish"] <= tb["n_wild_fish_high"]).all(), error
    # Check that the total agrees with the sum of aggregates from each continent, for non per capita columns.
    _tb = tb[[column for column in tb.columns if column not in "per_capita" not in column]].copy()
    # Get average global values between 2000 and 2019 (since country-level data are averages of that range).
    world = (
        _tb[(_tb["country"] == "World") & (_tb["year"] >= 2000) & (_tb["year"] <= 2019)]
        .groupby("country", as_index=False)
        .mean(numeric_only=True)
        .drop(columns=["country", "year"])
    )
    test = (
        _tb[_tb["country"].isin(["Africa", "North America", "South America", "Asia", "Europe", "Oceania"])]
        .groupby("year", as_index=False)
        .sum(numeric_only=True)
        .drop(columns=["year"])
    )
    assert (100 * abs(world - test) / world < 1).all().all()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its tables.
    ds_meadow = paths.load_dataset("number_of_wild_fish_killed_for_food")
    tb_global = ds_meadow.read("number_of_wild_fish_killed_for_food_global")
    tb_by_country = ds_meadow.read("number_of_wild_fish_killed_for_food_by_country")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    # Load population dataset.
    ds_population = paths.load_dataset("population")

    #
    # Process data.
    #
    # Select and rename columns in the table of global data, and add a country column.
    tb_global = (
        tb_global[list(COLUMNS_GLOBAL)].rename(columns=COLUMNS_GLOBAL, errors="raise").assign(**{"country": "World"})
    )

    # Select and rename columns in the table of country-level data, and add a year column.
    tb_by_country = (
        tb_by_country[list(COLUMNS_BY_COUNTRY)]
        .rename(columns=COLUMNS_BY_COUNTRY, errors="raise")
        .assign(**{"year": 2019})
    )

    # Prepare values and units.
    for column in ["n_wild_fish_low", "n_wild_fish_high", "n_wild_fish"]:
        # Global data is given in millions.
        tb_global[column] = tb_global[column].str.replace(",", "").astype(int) * 1e6
        tb_by_country[column] = tb_by_country[column].str.replace(",", "").astype(int)

    # Combine both tables.
    # NOTE: Data by country contains an "All countries combined" row. Ignore it.
    tb = pr.concat([tb_by_country[tb_by_country["country"] != "All countries combined"], tb_global])

    # Harmonize country names.
    # NOTE: Since the data is assumed to be for 2019, we exclude historical regions like Netherland Antilles.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Add region aggregates.
    tb = geo.add_regions_to_table(
        tb=tb, ds_regions=ds_regions, ds_income_groups=ds_income_groups, min_num_values_per_year=1
    )

    # Add per capita number of farmed fish.
    tb = add_per_capita_variables(tb=tb, ds_population=ds_population)

    # Run sanity checks on outputs.
    run_sanity_checks_on_outputs(tb=tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.format(sort_columns=True, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()

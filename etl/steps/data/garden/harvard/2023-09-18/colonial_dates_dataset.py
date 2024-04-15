"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define most recent year to extend the range of years of the dataset.
LATEST_YEAR = 2022


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("colonial_dates_dataset")

    # Read table from meadow dataset.
    tb = ds_meadow["colonial_dates_dataset"].reset_index()

    # Load population data.
    ds_pop = paths.load_dataset("population")
    tb_pop = ds_pop["population"].reset_index()

    #
    # Process data.
    #
    tb = process_data(tb, tb_pop)

    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Create regional aggregations for total_colonies
    tb = regional_aggregations(tb, tb_pop)

    # Make European countries not colonizers nor colonized as missing
    tb = correct_european_countries(tb)

    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def process_data(tb: Table, tb_pop: Table) -> Table:
    """Process data and create new columns."""

    # Capitalize colonizer column and replace Britain by United Kingdom
    tb["colonizer"] = tb["colonizer"].str.capitalize().replace("Britain", "United Kingdom")

    # Create two tables, one for colonized countries and one for colonizers/not colonized countries
    tb_colonized = tb[tb["col"] == "1"].reset_index(drop=True)
    tb_rest = tb[tb["col"] == "0"].reset_index(drop=True)

    # Get list of colonized countries and another list of colonizers
    colonized_list = tb_colonized["country"].unique().tolist()
    colonizers_list = tb_colonized["colonizer"].unique().tolist()

    # Filter tb_rest to only include countries that are not in colonized_list or colonizers_list
    tb_rest = tb_rest[~tb_rest["country"].isin(colonized_list + colonizers_list)].reset_index(drop=True)

    # Remove duplicates for tb_rest
    tb_rest = tb_rest.drop_duplicates(subset=["country"], keep="first").reset_index(drop=True)

    # Drop colonizer column from tb_rest
    tb_rest = tb_rest.drop(columns=["colonizer"])

    # For these countries, assign the minimum year of colstart_max as colstart_max and the maximum year of colend_max as colend_max
    tb_rest["colstart_max"] = tb_colonized["colstart_max"].min()
    tb_rest["colend_max"] = LATEST_YEAR

    # Create a year column with one value per row representing the range between colstart_max and colend_max
    # NOTE: I have decided to use last date aggregations, but we could also use mean aggregations
    tb_colonized["year"] = tb_colonized.apply(lambda x: list(range(x["colstart_max"], x["colend_max"] + 1)), axis=1)
    tb_rest["year"] = tb_rest.apply(lambda x: list(range(x["colstart_max"], x["colend_max"] + 1)), axis=1)

    # Explode the year column
    tb_colonized = tb_colonized.explode("year").reset_index(drop=True)
    tb_rest = tb_rest.explode("year").reset_index(drop=True)

    # Drop colstart, colend and col columns
    tb_colonized = tb_colonized.drop(columns=["colstart_max", "colend_max", "colstart_mean", "colend_mean", "col"])
    tb_rest = tb_rest.drop(columns=["colstart_max", "colend_max", "colstart_mean", "colend_mean", "col"])

    # Create another table with the total number of colonies per colonizer and year
    tb_count = tb_colonized.copy()
    tb_count = geo.harmonize_countries(df=tb_count, countries_file=paths.country_mapping_path)
    tb_count = tb_count.merge(tb_pop[["country", "year", "population"]], how="left", on=["country", "year"])

    tb_count = (
        tb_count.groupby(["colonizer", "year"], observed=True)
        .agg({"country": "count", "population": "sum"})
        .reset_index()
    )

    # Rename columns
    tb_count = tb_count.rename(
        columns={"colonizer": "country", "country": "total_colonies", "population": "total_colonies_pop"}
    )

    # Replace zeros with nan
    tb_count["total_colonies_pop"] = tb_count["total_colonies_pop"].replace(0, np.nan)

    # Consolidate results in country and year columns, by merging colonizer column in each row
    tb_colonized = (
        tb_colonized.groupby(["country", "year"])
        .agg({"colonizer": lambda x: " - ".join(x)})
        .reset_index()
        .copy_metadata(tb)
    )

    # Concatenate tb_colonized and tb_rest
    tb = pr.concat([tb_colonized, tb_rest, tb_count], short_name="colonial_dates_dataset")

    # Fill years in the range (tb_colonized['year'].min(), LATEST_YEAR) not present for each country
    tb = tb.set_index(["country", "year"]).unstack().stack(dropna=False).reset_index()

    # Create an additional summarized colonizer column, replacing the values with " - " with "More than one colonizer"
    # I add the "z." to have this at the last position of the map brackets
    tb["colonizer_grouped"] = tb["colonizer"].apply(
        lambda x: "z. Multiple colonizers" if isinstance(x, str) and " - " in x else x
    )

    # Create years_colonized: counts for each year the number of years a country has been colonized (e.g. there has been a non null colonizer)
    tb["years_colonized"] = tb.groupby(["country"])["colonizer"].transform(lambda x: x.notnull().cumsum())

    # Create last_colonizer column, which is the most recent non-null colonizer for each country and year
    tb["last_colonizer"] = tb.groupby(["country"])["colonizer"].fillna(method="ffill")
    tb["last_colonizer_grouped"] = tb.groupby(["country"])["colonizer_grouped"].fillna(method="ffill")

    # For countries in colonizers_list, assign the value "Colonizer" to colonizer, colonizer_grouped, last_colonizer and last_colonizer_group column
    for col in ["colonizer", "colonizer_grouped", "last_colonizer", "last_colonizer_grouped"]:
        tb[col] = tb[col].where(~tb["country"].isin(colonizers_list), "zz. Colonizer")
        tb[col] = tb[col].where(tb["country"].isin(colonized_list + colonizers_list), "zzz. Not colonized")
        tb[col] = tb[col].where(~tb[col].isnull(), "zzz. Not colonized")

    # For columns last_colonizer and last_colonizer_grouped, replace "zzz. Not colonized" by "zzz. Never colonized"
    for col in ["last_colonizer", "last_colonizer_grouped"]:
        tb[col] = tb[col].replace("zzz. Not colonized", "zzzz. Never colonized")

    # If colonizer is "zzz. Not colonized" and last_colonizer is different from "zzzz. Never colonized", assign last_colonizer to colonizer
    for col in ["colonizer", "colonizer_grouped"]:
        tb[col] = tb[col].where(
            ~((tb[col] == "zzz. Not colonized") & (tb["last_colonizer"] != "zzzz. Never colonized")),
            "zzzz. No longer colonized",
        )

    # For countries in colonizers_list total_colonies, assign 0 when it is null
    tb["total_colonies"] = tb["total_colonies"].where(
        ~((tb["country"].isin(colonizers_list)) & (tb["total_colonies"].isnull())), 0
    )

    # Add rows for the country "World" with the total number of colonies per year
    tb_count_world = tb.groupby(["year"]).agg({"total_colonies": "sum"}).reset_index()
    tb_count_world["country"] = "World"
    tb = pr.concat([tb, tb_count_world], short_name="colonial_dates_dataset")

    return tb


def regional_aggregations(tb: Table, tb_pop: Table) -> Table:
    """Create regional aggregations for total_colonies."""
    # Copy table
    tb_regions = tb.copy()

    # Merge population data.
    tb_regions = tb_regions.merge(tb_pop[["country", "year", "population"]], how="left", on=["country", "year"])

    # Define non-colonies identifiers for `colonizer`
    non_colonies = ["zz. Colonizer", "zzz. Not colonized", "zzzz. No longer colonized"]

    # Backwards compatibility
    tb_regions["colonizer"] = tb_regions["colonizer"].astype(object).fillna(np.nan)

    # Define colony_number, which is 1 if countries are not in non_colonies and colony_pop, which is the product of colony and population
    tb_regions["colony_number"] = tb_regions["colonizer"].apply(lambda x: 0 if x in non_colonies else 1)
    tb_regions["colony_pop"] = tb_regions["population"] * tb_regions["colony_number"]

    # Define colonizer_number, which is 1 if countries are in colonizers_list and colonizer_pop, which is the product of colonizer_bool and population
    tb_regions["colonizer_number"] = tb_regions["total_colonies"].apply(lambda x: 1 if x > 0 else 0)
    tb_regions["colonizer_pop"] = tb_regions["population"] * tb_regions["colonizer_number"]

    # Define former_colonizer_number, which is 1 if total_colonies = 0
    tb_regions["not_colonizer_number"] = tb_regions["total_colonies"].apply(lambda x: 1 if x == 0 else 0)
    tb_regions["not_colonizer_pop"] = tb_regions["population"] * tb_regions["not_colonizer_number"]

    # Define not_colonized_number, which is 1 if countries are in non_colonies and not_colonized_pop, which is the product of not_colonized and population
    tb_regions["not_colonized_number"] = tb_regions["colonizer"].apply(
        lambda x: 1 if x in ["zzz. Not colonized", "zzzz. No longer colonized"] else 0
    )
    tb_regions["not_colonized_pop"] = tb_regions["population"] * tb_regions["not_colonized_number"]

    # Define not_colonized_nor_colonizer_number, which is 1 if not_colonized_number or not_colonizer_number are 1
    tb_regions["not_colonized_nor_colonizer_number"] = (
        tb_regions["not_colonized_number"] + tb_regions["not_colonizer_number"]
    )
    tb_regions["not_colonized_nor_colonizer_pop"] = tb_regions["not_colonized_pop"] + tb_regions["not_colonizer_pop"]

    # Define regions to aggregate
    regions = [
        "Europe",
        "Asia",
        "North America",
        "South America",
        "Africa",
        "Oceania",
        "High-income countries",
        "Low-income countries",
        "Lower-middle-income countries",
        "Upper-middle-income countries",
        "European Union (27)",
        "World",
    ]

    # Define group of variables to generate
    var_list = [
        "colony_number",
        "colony_pop",
        "colonizer_number",
        "colonizer_pop",
        "not_colonized_nor_colonizer_number",
        "not_colonized_nor_colonizer_pop",
    ]

    # Define the variables and aggregation method to be used in the following function loop
    aggregations = dict.fromkeys(
        var_list + ["population"],
        "sum",
    )

    # Add regional aggregates, by summing up the variables in `aggregations`
    for region in regions:
        tb_regions = geo.add_region_aggregates(
            tb_regions,
            region=region,
            aggregations=aggregations,
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=0.2,
        )

    # Call the population data again to get regional total population
    tb_regions = tb_regions.merge(
        tb_pop[["country", "year", "population"]], how="left", on=["country", "year"], suffixes=("", "_region")
    )

    # Create an additional column with the population not considered in the dataset
    tb_regions["missing_pop"] = (
        tb_regions["population_region"]
        - tb_regions["colony_pop"]
        - tb_regions["colonizer_pop"]
        - tb_regions["not_colonized_nor_colonizer_pop"]
    )

    # Assert if missing_pop has negative values
    if tb_regions["missing_pop"].min() < 0:
        paths.log.warning(
            f"""`missing_pop` has negative values and will be replaced by 0.:
            {print(tb_regions[tb_regions["missing_pop"] < 0])}"""
        )
        # Replace negative values by 0
        tb_regions.loc[tb_regions["missing_pop"] < 0, "missing_pop"] = 0

    # Include missing_pop in not_colonized_nor_colonizer_pop
    tb_regions["not_colonized_nor_colonizer_pop"] = (
        tb_regions["not_colonized_nor_colonizer_pop"] + tb_regions["missing_pop"]
    )

    # Select only regions in tb_regions and "World" tb_world
    tb_world = tb_regions[tb_regions["country"] == "World"].reset_index(drop=True)
    tb_regions = tb_regions[(tb_regions["country"].isin(regions)) & (tb_regions["country"] != "World")].reset_index(
        drop=True
    )

    # Concatenate and merge
    tb = pr.merge(
        tb,
        tb_world[
            [
                "country",
                "year",
            ]
            + var_list
        ],
        on=["country", "year"],
        how="left",
    )
    tb = pr.concat([tb, tb_regions], short_name="colonial_dates_dataset")

    # Make variables in var_list integer
    for var in var_list:
        tb[var] = tb[var].astype("Int64")

    # Drop population column
    tb = tb.drop(
        columns=[
            "population",
            "population_region",
            "not_colonized_pop",
            "not_colonized_number",
            "not_colonizer_pop",
            "not_colonizer_number",
            "missing_pop",
        ]
    )

    return tb


def correct_european_countries(tb: Table) -> Table:
    """
    Make European countries not colonizers nor colonized as missing.
    This is because the dataset focuses on overseas territories (beyond Europe)
    """

    # Get list of European countries
    european_countries = geo.list_countries_in_region(region="Europe")

    # If the country is in european_countries and last_colonizer is not "zzzz. Never colonized", assign nan to colonizer
    for col in ["colonizer", "colonizer_grouped", "last_colonizer", "years_colonized", "last_colonizer_grouped"]:
        tb[col] = tb[col].where(
            ~((tb["country"].isin(european_countries)) & (tb["last_colonizer_grouped"] == "zzzz. Never colonized")),
            np.nan,
        )

    return tb

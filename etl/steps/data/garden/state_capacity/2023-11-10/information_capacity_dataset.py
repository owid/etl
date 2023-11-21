"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("information_capacity_dataset")

    # Read table from meadow dataset.
    tb = ds_meadow["information_capacity_dataset"].reset_index()

    # Load population data.
    ds_pop = paths.load_dataset("population")

    # Load regional data
    ds_regions = paths.load_dataset("regions")
    tb_country_list = ds_regions["regions"].reset_index()
    tb_country_list = tb_country_list[
        (tb_country_list["region_type"] == "country") & ~(tb_country_list["is_historical"])
    ].reset_index(drop=True)

    #
    # Process data.
    #

    # Drop country id columns
    tb = tb.drop(columns=["ccodecow", "vdemcode"])

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Reset index after dropping excluded countries
    tb = tb.reset_index(drop=True)

    # Add regional aggregations
    tb = regional_aggregations(tb, ds_pop, tb_country_list)

    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def regional_aggregations(tb: Table, ds_pop: Dataset, tb_country_list: Table) -> Table:
    """
    Add regional aggregations for some of the indicators
    """

    tb_regions = tb.copy()

    # Add missing countries to tb_regions
    tb_regions = add_missing_countries(tb_regions, tb_country_list)

    # Add population data
    tb_regions = geo.add_population_to_table(tb_regions, ds_pop)

    # List of index columns
    index_cols = ["censusgraded_ability", "ybcov_ability", "infcap_irt", "infcap_pca"]

    for col in index_cols:
        # Create new columns with the product of the index and the population
        tb_regions[col] = tb_regions[col].astype(float) * tb_regions["population"]

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

    # Define the variables and aggregation method to be used in the following function loop
    aggregations = dict.fromkeys(
        index_cols + ["population"],
        "sum",
    )

    # Add regional aggregates, by summing up the variables in `aggregations`
    for region in regions:
        tb_regions = geo.add_region_aggregates(
            tb_regions,
            region=region,
            aggregations=aggregations,
            countries_that_must_have_data=[],
            frac_allowed_nans_per_year=0.1,
        )

    # Filter table to keep only regions
    tb_regions = tb_regions[tb_regions["country"].isin(regions)].reset_index(drop=True)

    # Drop the population column from tb_regions
    tb_regions = tb_regions.drop(columns=["population"])

    # Call the population data again to get regional total population
    tb_regions = geo.add_population_to_table(tb_regions, ds_pop)

    # Divide index_cols_pop by population_region to get the index
    for col in index_cols:
        tb_regions[col] = tb_regions[col] / tb_regions["population"]

    # Drop columns
    tb_regions = tb_regions.drop(columns=["population"])

    # Concatenate tb and tb_regions
    tb = pr.concat([tb, tb_regions], ignore_index=True)

    # Delete all the rows with only nan values (except for country and year)
    tb = tb.dropna(how="all", subset=[col for col in tb.columns if col not in ["country", "year"]])

    return tb


def add_missing_countries(tb_regions: Table, tb_country_list: Table) -> Table:
    """
    Add countries not in the dataset to generate regional aggregates
    """

    # Add all the countries available in tb_country_list to tb_regions
    countries_tb = tb_regions["country"].unique().tolist()
    countries_regions_dataset = tb_country_list["name"].unique().tolist()

    # Obtain the list of countries in countries_regions_dataset that are not in countries_available
    countries_to_add = list(set(countries_regions_dataset) - set(countries_tb))

    # Make year integer
    tb_regions["year"] = tb_regions["year"].astype(int)

    # Create two tables, one for countries_to_add and one with the range between the minimum and maximum years
    tb_countries = Table({"country": countries_to_add})
    tb_years = Table({"year": range(tb_regions["year"].min(), tb_regions["year"].max() + 1)})

    # Create a new table with all the countries in country_list and all the years in year_list
    tb_country_year = pr.merge(tb_countries, tb_years, how="cross")

    # Add the new table to tb_regions
    tb_regions = pr.concat([tb_regions, tb_country_year], ignore_index=True)

    return tb_regions

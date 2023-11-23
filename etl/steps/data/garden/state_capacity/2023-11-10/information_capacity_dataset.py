"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd
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

    # Add new indicators
    tb = add_new_indicators(tb)

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Reset index after dropping excluded countries
    tb = tb.reset_index(drop=True)

    # Add regional aggregations
    tb = regional_aggregations(tb, tb_country_list)

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


def add_new_indicators(tb: Table) -> Table:
    """
    Add new indicators to the dataset, based on census and register_based_census
    """

    # Define minimum and maximum years
    tb["year"] = tb["year"].astype(int)
    min_year = tb["year"].min()
    max_year = tb["year"].max()

    # Define the list of countries
    countries = tb["country"].unique().tolist()

    # Create a table of years and another of countries
    tb_years = Table({"year": range(min_year, max_year + 1)})
    tb_countries = Table({"country": countries})

    # Create a new table with the cartesian product of tb_years and tb_countries
    tb_countries_years = pr.merge(tb_years, tb_countries, how="cross")

    # Merge this table with tb
    tb = pr.merge(tb_countries_years, tb, how="left", on=["country", "year"], short_name=tb.metadata.short_name)

    # Sort table by country and year
    tb = tb.sort_values(by=["country", "year"]).reset_index(drop=True)

    # Count the number of census (census column) and register-based census (register_based_census column) have been run in the previous 10 years for each country
    tb["census_10_years"] = (
        pd.DataFrame(tb).groupby("country", as_index=False)["census"].rolling(10, min_periods=1).sum()["census"]
    )
    tb["census_10_years"] = tb["census_10_years"].copy_metadata(tb["census"])

    tb["register_based_census_10_years"] = (
        pd.DataFrame(tb)
        .groupby("country", as_index=False)["register_based_census"]
        .rolling(10, min_periods=1)
        .sum()["register_based_census"]
    )
    tb["register_based_census_10_years"] = tb["register_based_census_10_years"].copy_metadata(
        tb["register_based_census"]
    )

    # For both variables, replace with 1 if values are greater than 1, and with 0 otherwise
    tb["census_10_years"] = tb["census_10_years"].apply(lambda x: 1 if x > 0 else 0)
    tb["register_based_census_10_years"] = tb["register_based_census_10_years"].apply(lambda x: 1 if x > 0 else 0)

    return tb


def regional_aggregations(tb: Table, tb_country_list: Table) -> Table:
    """
    Add regional aggregations for some of the indicators
    """

    tb_regions = tb.copy()

    # List of index columns
    cols_to_use = [
        "civreg",
        "popreg",
        "statagency",
        "census",
        "register_based_census",
        "census_10_years",
        "register_based_census_10_years",
    ]

    # Add "_region" to each item in cols to use and name it cols_to_agg
    cols_to_agg = [col + "_region" for col in cols_to_use]

    # Rename columns in tb_regions
    tb_regions = tb_regions.rename(columns=dict(zip(cols_to_use, cols_to_agg)))

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
        cols_to_agg,
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

    # Concatenate tb and tb_regions
    tb = pr.concat([tb, tb_regions], ignore_index=True)

    return tb

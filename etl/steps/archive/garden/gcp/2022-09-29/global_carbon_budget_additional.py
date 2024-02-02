"""This step creates a dataset that has additional variables that are currently not included in the Global Carbon
Budget (GCB) dataset (which was created in importers).

In the future (next time GCB dataset is updated and moved to ETL), a newer version of this step should create the
entire GCB dataset.

"""

from typing import Tuple, cast

import pandas as pd
from owid.catalog import Dataset, Table
from shared import CURRENT_DIR

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Regions and income groups to create (by aggregating), following OWID definitions.
REGIONS = [
    "Africa",
    "Asia",
    "Europe",
    "European Union (27)",
    "North America",
    "Oceania",
    "South America",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
]
# Variables to aggregate and type of aggregation to apply.
AGGREGATES = {"production_emissions": "sum", "consumption_emissions": "sum"}

# Naming conventions.
N = PathFinder(str(CURRENT_DIR / "global_carbon_budget_additional"))


def prepare_national_and_global_data(
    production_df: pd.DataFrame, consumption_df: pd.DataFrame, historical_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Separate and prepare national and global GCB data.

    Parameters
    ----------
    production_df : pd.DataFrame
        Production-based emissions (from the national data file).
    consumption_df : pd.DataFrame
        Consumption-based emissions (from the national data file).
    historical_df : pd.DataFrame
        Historical budget emissions (from the global data file).

    Returns
    -------
    national_df : pd.DataFrame
        Prepared national emissions data.
    globl_df : pd.DataFrame
        Prepared global emissions data.

    """
    production_df = production_df.copy()
    consumption_df = consumption_df.copy()
    historical_df = historical_df.copy()

    # In the original data, Bunkers was included in the national data file, as another country.
    # But I suppose it should be considered as another kind of global emission.
    # In fact, bunker emissions should coincide for production and consumption emissions.
    global_bunkers_emissions = (
        production_df[production_df["country"] == "Bunkers"][["year", "production_emissions"]]
        .reset_index(drop=True)
        .rename(columns={"production_emissions": "global_bunker_emissions"})
    )

    # Check that we get exactly the same array of bunker emissions from the consumption emissions dataframe.
    check = (
        consumption_df[consumption_df["country"] == "Bunkers"][["year", "consumption_emissions"]]
        .reset_index(drop=True)
        .rename(columns={"consumption_emissions": "global_bunker_emissions"})
    )
    error = "Bunker emissions were expected to coincide in production and consumption emissions dataframes."
    assert global_bunkers_emissions.equals(check), error

    # Now remove rows for bunker emissions from both production and consumption emissions.
    production_df = production_df[production_df["country"] != "Bunkers"].reset_index(drop=True)
    consumption_df = consumption_df[consumption_df["country"] != "Bunkers"].reset_index(drop=True)

    # Combine production and consumption dataframes.
    national_df = pd.merge(production_df, consumption_df, how="outer", on=["country", "year"])

    # Check that, for the World, production emissions coincides with consumption emissions.
    error = "Production and consumption emissions for the world were expected to be identical."
    assert (
        production_df[production_df["country"] == "World"]
        .reset_index(drop=True)["production_emissions"]
        .equals(consumption_df[consumption_df["country"] == "World"].reset_index(drop=True)["consumption_emissions"])
    ), error

    # Check that production emissions for the World coincide with global fossil emissions (from the historical dataframe).
    check = pd.merge(
        production_df[production_df["country"] == "World"][["year", "production_emissions"]].reset_index(drop=True),
        historical_df[["year", "global_fossil_emissions"]],
        how="inner",
        on="year",
    )
    error = "Production emissions for the world were expected to coincide with global fossil emissions."
    assert check[check["production_emissions"] != check["global_fossil_emissions"]].empty, error

    # Given that, we can ignore production and consumption emissions for the world, and take it from
    # the global fossil emissions (which has data since 1750 instead of 1959).
    complete_world_emissions = historical_df[["country", "year", "global_fossil_emissions"]].rename(
        columns={"global_fossil_emissions": "production_emissions"}
    )
    # Create an additional column of global consumption emissions (which, as we just checked, should be identical to
    # production emissions).
    complete_world_emissions["consumption_emissions"] = complete_world_emissions["production_emissions"]
    national_df = pd.concat(
        [national_df[national_df["country"] != "World"].reset_index(drop=True), complete_world_emissions],
        ignore_index=True,
    )

    # Add bunker emissions to the rest of global emissions.
    global_df = pd.merge(historical_df, global_bunkers_emissions, how="outer", on="year")

    # Add variable of total emissions including fossil fuels and land use change.
    global_df["global_fossil_and_land_use_change_emissions"] = (
        global_df["global_fossil_emissions"] + global_df["global_land_use_change_emissions"]
    )

    # Add global population.
    global_df = geo.add_population_to_dataframe(df=global_df, population_col="global_population")

    return cast(pd.DataFrame, national_df), cast(pd.DataFrame, global_df)


def add_per_capita_variables(national_df: pd.DataFrame) -> pd.DataFrame:
    """Add per capita variables to national emissions data.

    Parameters
    ----------
    national_df : pd.DataFrame
        National emissions data, after selecting variables and preparing them.

    Returns
    -------
    national_df : pd.DataFrame
        National emissions data, after adding per capita variables.

    """
    national_df = national_df.copy()

    # Add population to each country and year.
    national_df = geo.add_population_to_dataframe(df=national_df, warn_on_missing_countries=False)

    # Create per capita variables.
    national_df["consumption_emissions_per_capita"] = national_df["consumption_emissions"] / national_df["population"]
    national_df["production_emissions_per_capita"] = national_df["production_emissions"] / national_df["population"]

    return national_df


def add_share_variables(combined_df: pd.DataFrame) -> pd.DataFrame:
    """Add "share variables" (e.g. national emissions as share of global emissions).

    Parameters
    ----------
    combined_df : pd.DataFrame
        Combined dataframe of production and consumption based emissions (national data).

    Returns
    -------
    combined_df : pd.DataFrame
        Combined dataframe after adding share variables.

    """
    combined_df = combined_df.copy()

    # Create variables of production and consumption emissions as a share of global emissions.
    combined_df["production_emissions_as_share_of_global"] = (
        combined_df["production_emissions"] / combined_df["global_fossil_emissions"] * 100
    )
    combined_df["consumption_emissions_as_share_of_global"] = (
        combined_df["consumption_emissions"] / combined_df["global_fossil_emissions"] * 100
    )

    # Create variable of population as a share of global population.
    combined_df["population_as_share_of_global"] = combined_df["population"] / combined_df["global_population"] * 100

    # Sanity checks.
    error = "Production emissions as a share of global emissions should be 100% for 'World'."
    assert combined_df[
        (combined_df["country"] == "World") & (combined_df["production_emissions_as_share_of_global"] != 100)
    ].empty, error
    error = "Consumption emissions as a share of global emissions should be 100% for 'World'."
    assert combined_df[
        (combined_df["country"] == "World") & (combined_df["consumption_emissions_as_share_of_global"] != 100)
    ].empty, error
    error = "Population as a share of global population should be 100% for 'World'."
    assert combined_df[
        (combined_df["country"] == "World") & (combined_df["population_as_share_of_global"].fillna(100) != 100)
    ].empty, error

    return combined_df


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from meadow.
    ds_meadow = N.meadow_dataset
    # Load required tables with additional variables.
    consumption_tb = ds_meadow["consumption_emissions"]
    production_tb = ds_meadow["production_emissions"]
    historical_tb = ds_meadow["historical_emissions"]
    # Create a convenient dataframe for each table.
    production_df = pd.DataFrame(production_tb).reset_index()
    consumption_df = pd.DataFrame(consumption_tb).reset_index()
    historical_df = pd.DataFrame(historical_tb).reset_index()

    #
    # Process data.
    #
    # Separate national data (at the country level, although it includes "World") and global data.
    national_df, global_df = prepare_national_and_global_data(
        production_df=production_df, consumption_df=consumption_df, historical_df=historical_df
    )

    # Harmonize country names.
    national_df = (
        geo.harmonize_countries(
            df=national_df,
            countries_file=N.country_mapping_path,
            warn_on_missing_countries=False,
            make_missing_countries_nan=True,
        )
        .dropna(subset="country")
        .reset_index(drop=True)
    )

    # Add contributions from regions.
    for region in REGIONS:
        national_df = geo.add_region_aggregates(
            df=national_df,
            region=region,
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=0.9,
            aggregations=AGGREGATES,
        )

    # Add per capita variables.
    national_df = add_per_capita_variables(national_df=national_df)

    # Combine national and global variables.
    combined_df = pd.merge(national_df, global_df.drop(columns="country"), how="inner", on="year")

    # Add production and consumption emissions as a share of global emissions.
    combined_df = add_share_variables(combined_df=combined_df)

    # Set an index and sort conveniently.
    combined_df = combined_df.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset and use metadata from meadow dataset.
    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata
    ds_garden.metadata.short_name = N.short_name
    # Update metadata using the information in the yaml file.
    ds_garden.metadata.update_from_yaml(N.metadata_path, if_source_exists="replace")

    # Create a table with the combined data.
    tb_garden = Table(combined_df)
    # Use metadata from yaml file.
    tb_garden.update_metadata_from_yaml(N.metadata_path, "global_carbon_budget_additional")
    # Add combined table to garden dataset and save dataset.
    ds_garden.add(tb_garden)
    ds_garden.save()

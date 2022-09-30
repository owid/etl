"""This step creates a dataset that has additional variables that are currently not included in the Global Carbon
Budget (GCB) dataset (which was created in importers).

In the future (next time GCB dataset is updated and moved to ETL), a newer version of this step should create the
entire GCB dataset.

"""

import pandas as pd
from owid.catalog import Dataset, Table
from owid.datautils import dataframes, geo

from etl.helpers import Names

# naming conventions
N = Names(__file__)


def combine_data(production_df: pd.DataFrame, consumption_df: pd.DataFrame, historical_df: pd.DataFrame) -> pd.DataFrame:
    production_df = production_df.copy()
    consumption_df = consumption_df.copy()
    historical_df = historical_df.copy()
    # Add column for the sum of global fossil emissions and global land-use change emissions.
    historical_df["global_fossil_and_land_use_change_emissions"] = historical_df["global_fossil_emissions"] + historical_df["global_land_use_change_emissions"]
    # Bunker emissions should coincide for production and consumption emissions.
    # Keep both columns for now to check that this is true (and remove it after checking).
    production_df = production_df.rename(columns={"bunker_emissions": "check"})
    # Combine all dataframes.
    combined_df = dataframes.multi_merge(dfs=[production_df, consumption_df, historical_df], how="outer", on=["country", "year"])
    # Check there are no repeated columns (if so, the merge would have produced at least one column named "*_x").
    error = "There were unexpected repeated columns between different dataframes."
    assert len([column for column in combined_df.columns if column.endswith("_x")]) == 0, error
    # Check that bunker fuels coincide in production and consumption emissions.
    error = "Bunker emissions were expected to coincide in production and consumption emissions dataframes."
    assert combined_df[combined_df["bunker_emissions"].fillna(0) != combined_df["check"].fillna(0)].empty, error
    # Drop that column used only as a sanity check.
    combined_df = combined_df.drop(columns="check")

    return combined_df


def add_per_capita_variables(combined_df: pd.DataFrame) -> pd.DataFrame:
    combined_df = combined_df.copy()
    # Add per capita variables.
    combined_df = geo.add_population_to_dataframe(df=combined_df, warn_on_missing_countries=False)
    # Create per capita variables.
    combined_df["consumption_emissions_per_capita"] = combined_df["consumption_emissions"] / combined_df["population"]
    combined_df["production_emissions_per_capita"] = combined_df["production_emissions"] / combined_df["population"]

    return combined_df


def add_share_variables(combined_df: pd.DataFrame) -> pd.DataFrame:
    combined_df = combined_df.copy()
    # Check that global production and consumption emissions coincide.
    global_emissions = combined_df[combined_df["country"] == "World"][["year", "production_emissions"]]
    check = combined_df[combined_df["country"] == "World"][["year", "consumption_emissions"]]
    error = "Global production and consumption emissions were expected to coincide."
    assert (global_emissions["production_emissions"].fillna(0) == check["consumption_emissions"].fillna(0)).all(), error

    # Add global emissions as a new column of the combined dataframe.
    combined_df = pd.merge(combined_df, global_emissions.rename(columns={"production_emissions": "global_emissions"}), how="left", on="year")
    # Create variables of production and consumption emissions as a share of global emissions.
    combined_df["production_emissions_as_share_of_global"] = combined_df["production_emissions"] / combined_df["global_emissions"] * 100
    combined_df["consumption_emissions_as_share_of_global"] = combined_df["consumption_emissions"] / combined_df["global_emissions"] * 100

    # Add global population as a new column of the combined dataframe.
    global_population = combined_df[combined_df["country"] == "World"][["year", "population"]]
    combined_df = pd.merge(combined_df, global_population.rename(columns={"population": "global_population"}), how="left", on="year")
    # Create variable of population as a share of global population.
    combined_df["population_as_share_of_global"] = combined_df["population"] / combined_df["global_population"] * 100

    # Sanity checks.
    error = "Production emissions as a share of global emissions should be 100% for 'World'."
    assert (combined_df[combined_df["country"] == "World"]["production_emissions_as_share_of_global"].fillna(100).unique() == [100]).all(), error
    error = "Consumption emissions as a share of global emissions should be 100% for 'World'."
    assert (combined_df[combined_df["country"] == "World"]["consumption_emissions_as_share_of_global"].fillna(100).unique() == [100]).all(), error
    error = "Population as a share of global population should be 100% for 'World'."
    assert (combined_df[combined_df["country"] == "World"]["population_as_share_of_global"].fillna(100).unique() == [100]).all(), error

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
    # Combine all dataframes.
    combined_df = combine_data(production_df=production_df, consumption_df=consumption_df, historical_df=historical_df)
    # Harmonize country names.
    combined_df = geo.harmonize_countries(df=combined_df, countries_file=N.country_mapping_path)
    # Add per capita variables.
    combined_df = add_per_capita_variables(combined_df=combined_df)
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
    # Update metadata using the information in the yaml file.
    ds_garden.metadata.update_from_yaml(N.metadata_path, if_source_exists="replace")
    # Create a table with the combined data.
    tb_garden = Table(combined_df)
    # Use metadata from yaml file.
    tb_garden.update_metadata_from_yaml(N.metadata_path, "global_carbon_budget_additional")
    # Add combined table to garden dataset and save dataset.
    ds_garden.add(tb_garden)
    ds_garden.save()

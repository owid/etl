"""This step creates the Global Carbon Budget (GCB) dataset, by the Global Carbon Project (GCP).

"""

from typing import Tuple

import numpy as np
import pandas as pd
from owid import catalog
from owid.datautils import dataframes, geo

from etl.paths import DATA_DIR, STEP_DIR

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

# Define inputs.
# Country names harmonization file for fossil CO2 emissions data.
CO2_COUNTRIES_FILE = STEP_DIR / "data/garden/gcp/2022-11-11/global_carbon_budget.countries.json"
# Country names harmonization file for additional data.
ADDITIONAL_COUNTRIES_FILE = STEP_DIR / "data/garden/gcp/2022-09-29/global_carbon_budget_additional.countries.json"
# Meadow dataset on GCB fossil CO2 emissions.
MEADOW_CO2_DATASET_PATH = DATA_DIR / f"meadow/gcp/2022-11-11/global_carbon_budget_fossil_co2_emissions"
# Meadow dataset on GCB additional data (consumption-based emissions, land-use change and bunker emissions).
MEADOW_ADDITIONAL_DATASET_PATH = DATA_DIR / f"meadow/gcp/2022-09-29/global_carbon_budget_additional"
# Garden dataset on primary energy consumption.
GARDEN_PRIMARY_ENERGY_DATASET_PATH = DATA_DIR / "garden/energy/2022-07-29/primary_energy_consumption"
# Garden dataset on GDP.
GARDEN_GDP_DATASET_PATH = DATA_DIR / "garden/ggdc/2020-10-01/ggdc_maddison"
# Additionally, population dataset and income groups are also used (through datautils.geo functions).

# Define outputs.
# Name of output dataset.
DATASET_NAME = "global_carbon_budget"
# Path to metadata file.
METADATA_PATH = STEP_DIR / "data/garden/gcp/2022-11-11/global_carbon_budget.meta.yml"

# Label used for international transport (emissions from oil in bunker fuels), included as a country in the
# fossil CO2 emissions dataset.
INTERNATIONAL_TRANSPORT_LABEL = "International Transport"

# Columns to use from GCB fossil CO2 emissions data and how to rename them.
CO2_COLUMNS = {
    "country": "country",
    "year": "year",
    "cement": "emissions_from_cement",
    "coal": "emissions_from_coal",
    "flaring": "emissions_from_flaring",
    "gas": "emissions_from_gas",
    "oil": "emissions_from_oil",
    "other": "emissions_from_other_industry",
    "total": "emissions_total",
}

# List all sources of emissions considered.
EMISSION_SOURCES = [column for column in CO2_COLUMNS.values() if column not in ["country", "year"]]

# Columns to use from primary energy consumption data and how to rename them.
PRIMARY_ENERGY_COLUMNS = {
    "country": "country",
    "year": "year",
    "primary_energy_consumption__twh": "primary_energy_consumption",
}

# Columns to use from GDP data and how to rename them.
GDP_COLUMNS = {
    "country": "country",
    "year": "year",
    "gdp": "gdp",
}

# Conversion from terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9

# Convert from million tonnes of carbon to million tonnes of CO2.
MILLION_TONNES_OF_CARBON_TO_MILLION_TONNES_CO2 = 3.664

# Conversion from tonnes of CO2 to million tonnes of CO2.
# NOTE: This conversion factor is needed because, in the meadow step of additional data, variables were converted
#  from million tonnes to tonnes. In the next update, we could omit that conversion there and here.
TONNES_OF_CO2_TO_MILLION_TONNES_OF_CO2 = 1e-6


# TODO: Adapt make these sanity checks work.
def sanity_checks(
    production_df: pd.DataFrame, consumption_df: pd.DataFrame, historical_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
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

    # Check that, for the World, production emissions coincides with consumption emissions.
    error = "Production and consumption emissions for the world were expected to be identical."
    assert (
        production_df[production_df["country"] == "World"]
        .reset_index(drop=True)["production_emissions"]
        .equals(consumption_df[consumption_df["country"] == "World"].reset_index(drop=True)["consumption_emissions"])
    ), error

    # Check that production emissions for the World coincide with global fossil emissions (from historical dataframe).
    check = pd.merge(
        production_df[production_df["country"] == "World"][["year", "production_emissions"]].reset_index(drop=True),
        historical_df[["year", "global_fossil_emissions"]],
        how="inner",
        on="year",
    )
    error = "Production emissions for the world were expected to coincide with global fossil emissions."
    assert check[check["production_emissions"] != check["global_fossil_emissions"]].empty, error

    # TODO: The following "combined_df" could be an input (which is the final co2_df after reset_index()).
    
    combined_df = combined_df.copy()

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


def extract_global_emissions(co2_df: pd.DataFrame, historical_df: pd.DataFrame) -> pd.DataFrame:
    # NOTE: This function has to be used after selecting and renaming columns in co2_df, but before harmonizing
    # country names in co2_df (so that "International Transport" is still listed as a country).
    # For some reason, "International Transport" is included as another country, that only has emissions from oil.
    # We separate it as another variable (only given at the global level).
    global_transport = co2_df[co2_df["country"] == INTERNATIONAL_TRANSPORT_LABEL].reset_index(drop=True)

    # Check that total emissions for international transport coincide with oil emissions.
    error = "Total emissions from international transport do not coincide with oil emissions."
    assert all((global_transport["emissions_from_oil"] -\
                global_transport["emissions_total"]).dropna() == 0), error

    # Therefore, we can keep only one column for international transport emissions.
    global_transport = global_transport[["year", "emissions_from_oil"]].dropna().rename(columns={
        "emissions_from_oil": "global_emissions_from_international_transport"})

    # Create a new dataframe of global emissions.
    global_emissions = co2_df[co2_df["country"].isin(["Global", "World"])][["year"] + EMISSION_SOURCES].\
        rename(columns={column: f"global_{column}" for column in EMISSION_SOURCES}).sort_values("year").\
        reset_index(drop=True)

    # Calculate global cumulative emissions.
    for column in EMISSION_SOURCES:
        global_emissions[f"global_cumulative_{column}"] = global_emissions[f"global_{column}"].cumsum()

    # Add bunker fuels to global emissions.
    global_emissions = pd.merge(global_emissions, global_transport, on=["year"], how="outer")

    # Prepare dataframe of historical emissions.
    # For convenience, rename land-use change column and ensure it has the right units.
    _historical_df = historical_df.rename(columns={
        "global_land_use_change_emissions": "global_emissions_from_land_use_change"}).drop(columns="country")
    _historical_df["global_emissions_from_land_use_change"] *= TONNES_OF_CO2_TO_MILLION_TONNES_OF_CO2

    # Add historical land-use change emissions to dataframe of global emissions.
    global_emissions = pd.merge(global_emissions, _historical_df, how="left", on="year")

    # Add variable of total emissions including fossil fuels and land use change.
    global_emissions["global_emissions_total_including_land_use_change_emissions"] = (
        global_emissions["global_emissions_total"] + global_emissions["global_emissions_from_land_use_change"]
    )

    # Add a country column and add global population.
    global_emissions["country"] = "World"

    # Add global population.
    global_emissions = geo.add_population_to_dataframe(df=global_emissions, population_col="global_population")

    return global_emissions


def harmonize_co2_data(co2_df: pd.DataFrame) -> pd.DataFrame:
    # Harmonize country names in fossil CO2 data.
    # Remove "International Transport" from list of countries
    # NOTE: If we end up adding a list of excluded countries, this could be included.
    co2_df = co2_df[co2_df["country"] != INTERNATIONAL_TRANSPORT_LABEL].reset_index(drop=True)
    co2_df = geo.harmonize_countries(df=co2_df, countries_file=CO2_COUNTRIES_FILE,
                            warn_on_missing_countries=True, warn_on_unused_countries=True)

    # After harmonization, "Pacific Islands (Palau)" is mapped to "Palau", and therefore there are rows with different
    # data for the same country-year.
    # However, "Pacific Islands (Palau)" have data until 1991, and "Palau" has data from 1992 onwards.
    # Check that they don't have (non-zero) data on the same years.
    error = "Countries 'Pacific Islands (Palau)' and 'Palau' should not have data in overlapping years."
    assert set(co2_df[(co2_df["country"] == "Pacific Islands (Palau)") & (co2_df["emissions_total"] != 0)]["year"]) &\
        set(co2_df[(co2_df["country"] == "Palau") & (co2_df["emissions_total"] != 0)]["year"]) == set()

    # Combine Palau data (after converting zeros into nan) with the entire dataframe, prioritising the former.
    # This way, wherever we have Palau data, we will use it, and wherever we have only nan, we will keep not-nan data.
    # For rows where both duplicates of Palau are nan we will keep any of them.
    co2_df = dataframes.combine_two_overlapping_dataframes(
        df1=co2_df[co2_df["country"] == "Palau"].replace(0, np.nan),
        df2=co2_df, index_columns=["country", "year"]).reset_index(drop=True)

    # Check that the only duplicated rows found are for "Palau".
    assert co2_df[co2_df.duplicated(subset=["country", "year"])]["country"].unique().tolist() == ["Palau"]

    # Remove duplicated rows.
    co2_df = co2_df.drop_duplicates(subset=["country", "year"], keep="last").reset_index(drop=True)

    return co2_df


# TODO: Consider refactoring into small functions to simplify the following code.
def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load fossil CO2 emissions dataset from meadow.
    co2_ds = catalog.Dataset(MEADOW_CO2_DATASET_PATH)
    # Load main table from CO2 dataset.
    co2_tb = co2_ds[co2_ds.table_names[0]]
    # Create a dataframe out of the CO2 table.
    co2_df = pd.DataFrame(co2_tb).reset_index()

    # Load additional data (consumption-based emissions, global land-use change emissions, and bunker emissions).
    additional_ds = catalog.Dataset(MEADOW_ADDITIONAL_DATASET_PATH)
    # Load required tables with additional variables.
    consumption_tb = additional_ds["consumption_emissions"]
    production_tb = additional_ds["production_emissions"]
    historical_tb = additional_ds["historical_emissions"]
    # Create a convenient dataframe for each table.
    production_df = pd.DataFrame(production_tb).reset_index()
    consumption_df = pd.DataFrame(consumption_tb).reset_index()
    historical_df = pd.DataFrame(historical_tb).reset_index()

    # Load primary energy consumption from garden.
    primary_energy_ds = catalog.Dataset(GARDEN_PRIMARY_ENERGY_DATASET_PATH)
    # Create a  dataframe out of the main table of primary energy.
    primary_energy_df = pd.DataFrame(primary_energy_ds[primary_energy_ds.table_names[0]]).reset_index()

    # Load GDP dataset from garden.
    gdp_ds = catalog.Dataset(GARDEN_GDP_DATASET_PATH)
    # Create a dataframe out of the main table of GDP.
    gdp_df = pd.DataFrame(gdp_ds[gdp_ds.table_names[0]]).reset_index()

    #
    # Process data.
    #
    # Select and rename columns from primary energy data.
    primary_energy_df = primary_energy_df[list(PRIMARY_ENERGY_COLUMNS)].rename(columns=PRIMARY_ENERGY_COLUMNS)

    # Select and rename columns from primary energy data.
    gdp_df = gdp_df[list(GDP_COLUMNS)].rename(columns=GDP_COLUMNS)

    # Select and rename columns from fossil CO2 data.
    co2_df = co2_df[list(CO2_COLUMNS)].rename(columns=CO2_COLUMNS)

    # For some reason, "International Transport" is included as another country, that only has emissions from oil.
    # Extract that data and remove it from the rest of national emissions.
    global_emissions = extract_global_emissions(co2_df=co2_df, historical_df=historical_df)

    # Harmonize country names in consumption-based emissions data.
    consumption_df = geo.harmonize_countries(
        df=consumption_df, countries_file=ADDITIONAL_COUNTRIES_FILE, warn_on_missing_countries=False,
        make_missing_countries_nan=True).dropna(subset="country").reset_index(drop=True)

    # Harmonize country names in production-based emissions data.
    production_df = geo.harmonize_countries(
        df=production_df, countries_file=ADDITIONAL_COUNTRIES_FILE, warn_on_missing_countries=False,
        make_missing_countries_nan=True).dropna(subset="country").reset_index(drop=True)

    # Harmonize fossil CO2 data.
    co2_df = harmonize_co2_data(co2_df=co2_df)

    # Convert all variable units from million tonnes of carbon to million tonnes of CO2.
    co2_df[EMISSION_SOURCES] *= MILLION_TONNES_OF_CARBON_TO_MILLION_TONNES_CO2

    # Add consumption emissions to main dataframe (keep only the countries of the main dataframe).
    co2_df = pd.merge(co2_df, consumption_df, on=["country", "year"], how="left")

    # Add region aggregates.
    aggregations = {column: "sum" for column in EMISSION_SOURCES + ["consumption_emissions"]}
    for region in REGIONS:
        co2_df = geo.add_region_aggregates(
            df=co2_df, region=region, countries_that_must_have_data=[], frac_allowed_nans_per_year=0.999,
            aggregations=aggregations)

    # Add population to dataframe.
    co2_df = geo.add_population_to_dataframe(df=co2_df, warn_on_missing_countries=False)

    # Add GDP to main dataframe.
    co2_df = pd.merge(co2_df, gdp_df, on=["country", "year"], how="left")

    # Add primary energy to main dataframe.
    co2_df = pd.merge(co2_df, primary_energy_df, on=["country", "year"], how="left")

    # Add global emissions and global cumulative emissions columns to main dataframe.
    co2_df = pd.merge(co2_df, global_emissions.drop(columns="country"), on=["year"], how="left")

    # Ensure main dataframe is sorted (so that cumulative emissions are properly calculated).
    co2_df = co2_df.sort_values(["country", "year"]).reset_index(drop=True)

    # Temporarily add variables "global_consumption_emissions" and "global_cumulative_consumption_emissions".
    # This is done simply to be able to consider "consumption_emissions" as just another type of emission
    # when creating additional variables.
    co2_df["global_consumption_emissions"] = co2_df["global_emissions_total"]
    co2_df["global_cumulative_consumption_emissions"] = co2_df["global_cumulative_emissions_total"]

    # Add new variables for each source of emissions.
    for column in EMISSION_SOURCES + ["consumption_emissions"]:
        # Add per-capita variables.
        co2_df[f"{column}_per_capita"] = co2_df[column] / co2_df["population"]

        # Add columns for cumulative emissions.
        # Rows that had nan emissions will have nan cumulative emissions.
        # But nans will not be propagated in the sum.
        # This means that countries with some (not all) nans will have the cumulative sum of the informed emissions
        # (treating nans as zeros), but will have nan on those rows that were not informed.
        co2_df[f"cumulative_{column}"] = co2_df.groupby(["country"])[column].cumsum()

        # Add share of global emissions.
        co2_df[f"{column}_as_share_of_global"] = 100 * co2_df[column] / co2_df[f"global_{column}"]

        # Add share of global cumulative emissions.
        co2_df[f"cumulative_{column}_as_share_of_global"] = 100 * co2_df[f"cumulative_{column}"] /\
            co2_df[f"global_cumulative_{column}"]

    # Add total emissions per unit energy (in kg of emissions per kWh).
    co2_df["emissions_total_per_unit_energy"] = co2_df["emissions_total"] /\
        (co2_df["primary_energy_consumption"] * TWH_TO_KWH)

    # Add total emissions per unit GDP.
    co2_df["emissions_total_per_gdp"] = co2_df["emissions_total"] / co2_df["gdp"]

    # TODO: Considering renaming "consumption_emissions" -> "consumption_emissions_total",
    #  and "emissions" -> "production_emissions".
    # Add total consumption emissions per unit GDP.
    co2_df["consumption_emissions_per_gdp"] = co2_df["consumption_emissions"] / co2_df["gdp"]

    # Add variable of emissions embedded in trade.
    co2_df["traded_emissions"] = co2_df["consumption_emissions"] - co2_df["emissions_total"]
    co2_df["pct_traded_emissions"] = 100 * (co2_df["traded_emissions"] / co2_df["emissions_total"])
    co2_df["traded_emissions_per_capita"] = co2_df["traded_emissions"] / co2_df["population"]
    
    # Remove temporary columns.
    co2_df = co2_df.drop(columns=["global_consumption_emissions", "global_cumulative_consumption_emissions"])

    # Add annual percentage growth of total emissions.
    co2_df["pct_growth_emissions_total"] = co2_df.groupby("country")["emissions_total"].pct_change() * 100

    # Add annual absolute growth of total emissions.
    co2_df["growth_emissions_total"] = co2_df.groupby("country")["emissions_total"].diff()

    # Create variable of population as a share of global population.
    co2_df["population_as_share_of_global"] = co2_df["population"] / co2_df["global_population"] * 100

    # Set an appropriate index, ensure there are no rows that only have nan, and sort conveniently.
    co2_df = co2_df.set_index(["country", "year"], verify_integrity=True)
    co2_df = co2_df.dropna(subset=co2_df.columns, how="all").sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset and use metadata from meadow dataset.
    ds_garden = catalog.Dataset.create_empty(dest_dir)
    ds_garden.metadata = co2_ds.metadata
    # Update metadata using the information in the yaml file.
    ds_garden.metadata.update_from_yaml(METADATA_PATH, if_source_exists="replace")

    # Create a table with the combined data.
    tb_garden = catalog.Table(co2_df)
    # Use metadata from yaml file.
    tb_garden.update_metadata_from_yaml(METADATA_PATH, DATASET_NAME)
    # Add combined table to garden dataset and save dataset.
    ds_garden.add(tb_garden)
    ds_garden.save()

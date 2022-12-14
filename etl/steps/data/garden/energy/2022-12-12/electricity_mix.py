"""Garden step that combines BP's statistical review with Ember's combined (Global & European) Electricity Review
to create the Electricity Mix (BP & Ember) dataset.

"""

from typing import Dict, List

import pandas as pd
from owid import catalog
from owid.datautils.dataframes import combine_two_overlapping_dataframes
from shared import CURRENT_DIR, add_population

from etl.paths import DATA_DIR

# Details for dataset to export.
DATASET_SHORT_NAME = "electricity_mix"
METADATA_PATH = CURRENT_DIR / f"{DATASET_SHORT_NAME}.meta.yml"
# Details for datasets to import.
BP_DATASET_PATH = DATA_DIR / "garden/bp/2022-07-14/statistical_review"
EMBER_DATASET_PATH = DATA_DIR / "garden/ember/2022-08-01/combined_electricity_review"

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9
# Megatonnes to grams.
MT_TO_G = 1e12


def process_bp_data(table_bp: catalog.Table) -> pd.DataFrame:
    """Load necessary columns from BP's Statistical Review dataset, and create some new variables (e.g. electricity
    generation from fossil fuels).

    Parameters
    ----------
    table_bp : catalog.Table
        BP's Statistical Review (already processed, with harmonized countries and region aggregates).

    Returns
    -------
    df_bp : pd.DataFrame
        Processed BP data.

    """
    # Columns to load from BP dataset.
    columns = {
        "electricity_generation": "total_generation__twh",
        "primary_energy_consumption__twh": "primary_energy_consumption__twh",
        "hydro_generation__twh": "hydro_generation__twh",
        "nuclear_generation__twh": "nuclear_generation__twh",
        "solar_generation__twh": "solar_generation__twh",
        "wind_generation__twh": "wind_generation__twh",
        "geo_biomass_other__twh": "other_renewables_including_bioenergy_generation__twh",
        "elec_gen_from_oil": "oil_generation__twh",
        "elec_gen_from_coal": "coal_generation__twh",
        "elec_gen_from_gas": "gas_generation__twh",
    }
    table_bp = table_bp[list(columns)].rename(columns=columns, errors="raise")
    # New columns to be created by summing other columns.
    aggregates: Dict[str, List[str]] = {
        "fossil_generation__twh": [
            "oil_generation__twh",
            "coal_generation__twh",
            "gas_generation__twh",
        ],
        "renewable_generation__twh": [
            "hydro_generation__twh",
            "solar_generation__twh",
            "wind_generation__twh",
            "other_renewables_including_bioenergy_generation__twh",
        ],
        "low_carbon_generation__twh": [
            "renewable_generation__twh",
            "nuclear_generation__twh",
        ],
    }

    # Create a dataframe with a dummy index.
    df_bp = pd.DataFrame(table_bp).reset_index()

    # Create new columns, by adding up other columns (and allowing for only one nan in each sum).
    for new_column in aggregates:
        df_bp[new_column] = df_bp[aggregates[new_column]].sum(axis=1, min_count=len(aggregates[new_column]) - 1)

    return df_bp


def process_ember_data(table_ember: catalog.Table) -> pd.DataFrame:
    """Load necessary columns from the Combined Electricity Review and prepare a dataframe with the required variables.

    Parameters
    ----------
    table_ember : catalog.Table
        Combined Electricity Review (combination of Ember's Global and European Electricity Reviews).

    Returns
    -------
    df_ember : pd.DataFrame
        Processed Combined Electricity Review.

    """
    # Columns to load from Ember dataset.
    columns = {
        "generation__bioenergy__twh": "bioenergy_generation__twh",
        "generation__gas__twh": "gas_generation__twh",
        "generation__coal__twh": "coal_generation__twh",
        "generation__other_fossil__twh": "oil_generation__twh",
        "generation__renewables__twh": "renewable_generation__twh",
        "generation__other_renewables__twh": "other_renewables_excluding_bioenergy_generation__twh",
        "generation__clean__twh": "low_carbon_generation__twh",
        "generation__hydro__twh": "hydro_generation__twh",
        "generation__nuclear__twh": "nuclear_generation__twh",
        "generation__solar__twh": "solar_generation__twh",
        "generation__wind__twh": "wind_generation__twh",
        "generation__fossil__twh": "fossil_generation__twh",
        "generation__total_generation__twh": "total_generation__twh",
        "demand__total_demand__twh": "total_demand__twh",
        "emissions__total_emissions__mtco2": "total_emissions__mtco2",
        "emissions__co2_intensity__gco2_kwh": "co2_intensity__gco2_kwh",
        "imports__total_net_imports__twh": "total_net_imports__twh",
    }
    table_ember = table_ember[list(columns)].rename(columns=columns, errors="raise")

    # Create a dataframe with a dummy index.
    df_ember = pd.DataFrame(table_ember).reset_index()

    # In BP data, there is a variable "Geo Biomass Other", which combines all other renewables.
    # In Ember data, "other rewenables" excludes bioenergy.
    # To be able to combine both datasets, create a new variable for generation of other renewables including bioenergy.
    df_ember["other_renewables_including_bioenergy_generation__twh"] = (
        df_ember["other_renewables_excluding_bioenergy_generation__twh"] + df_ember["bioenergy_generation__twh"]
    )

    return df_ember


def add_per_capita_variables(combined: pd.DataFrame) -> pd.DataFrame:
    """Add per capita variables (in kWh per person) to the combined BP and Ember dataframe.

    The list of variables to make per capita are given in this function. The new variable names will be 'per_capita_'
    followed by the original variable's name.

    Parameters
    ----------
    combined : pd.DataFrame
        Combination of BP's Statistical Review and Ember's Combined Electricity Review.

    Returns
    -------
    combined : pd.DataFrame
        Input dataframe after adding per capita variables.

    """
    combined = combined.copy()

    # Variables to make per capita.
    per_capita_variables = [
        "bioenergy_generation__twh",
        "coal_generation__twh",
        "fossil_generation__twh",
        "gas_generation__twh",
        "hydro_generation__twh",
        "low_carbon_generation__twh",
        "nuclear_generation__twh",
        "oil_generation__twh",
        "other_renewables_excluding_bioenergy_generation__twh",
        "other_renewables_including_bioenergy_generation__twh",
        "renewable_generation__twh",
        "solar_generation__twh",
        "total_generation__twh",
        "wind_generation__twh",
    ]
    # Add a column for population (only for harmonized countries).
    combined = add_population(df=combined, warn_on_missing_countries=False)

    for variable in per_capita_variables:
        assert "twh" in variable, f"Variables are assumed to be in TWh, but {variable} is not."
        new_column = "per_capita_" + variable.replace("__twh", "__kwh")
        combined[new_column] = combined[variable] * TWH_TO_KWH / combined["population"]

    return combined


def add_share_variables(combined: pd.DataFrame) -> pd.DataFrame:
    """Add variables for the electricity generation as a share of the total electricity generation (as a percentage).

    The following new variables will be created:
    * For each source (e.g. coal_generation__twh) in a list given in this function, a new variable will be created
      (named, e.g. coal_share_of_electricity__pct).
    * Total electricity generation as a share of primary energy consumption.
    * Total net electricity imports as a share of total electricity demand.

    Parameters
    ----------
    combined : pd.DataFrame
        Combination of BP's Statistical Review and Ember's Combined Electricity Review.

    Returns
    -------
    combined : pd.DataFrame
        Input dataframe after adding share variables.

    """
    # Variables to make as share of electricity (new variable names will be the name of the original variable followed
    # by '_share_of_electricity__pct').
    share_variables = [
        "bioenergy_generation__twh",
        "coal_generation__twh",
        "fossil_generation__twh",
        "gas_generation__twh",
        "hydro_generation__twh",
        "low_carbon_generation__twh",
        "nuclear_generation__twh",
        "oil_generation__twh",
        "other_renewables_excluding_bioenergy_generation__twh",
        "other_renewables_including_bioenergy_generation__twh",
        "renewable_generation__twh",
        "solar_generation__twh",
        "total_generation__twh",
        "wind_generation__twh",
    ]
    for variable in share_variables:
        new_column = variable.replace("_generation__twh", "_share_of_electricity__pct")
        combined[new_column] = 100 * combined[variable] / combined["total_generation__twh"]

    # Calculate the percentage of electricity as a share of primary energy.
    combined["total_electricity_share_of_primary_energy__pct"] = (
        100 * combined["total_generation__twh"] / combined["primary_energy_consumption__twh"]
    )

    # Calculate the percentage of electricity demand that is imported.
    combined["net_imports_share_of_demand__pct"] = (
        100 * combined["total_net_imports__twh"] / combined["total_demand__twh"]
    )

    # Sanity check.
    error = "Total electricity share does not add up to 100%."
    assert all(abs(combined["total_share_of_electricity__pct"].dropna() - 100) < 0.01), error

    # Remove unnecessary columns.
    combined = combined.drop(columns=["total_share_of_electricity__pct"])

    return combined


def prepare_output_table(combined: pd.DataFrame) -> catalog.Table:
    """Convert the combined (BP + Ember) dataframe into a table with the appropriate metadata and variables metadata.

    Parameters
    ----------
    combined : pd.DataFrame
        BP's Statistical Review combined with Ember's Combined Electricity Review, after adding per capita variables and
        share variables.

    Returns
    -------
    table : catalog.Table
        Original data in a table format with metadata.

    """
    # Set an appropriate index and sort rows and columns conveniently.
    combined = combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Convert dataframe into a table (with no metadata).
    table = catalog.Table(combined)

    # Load metadata from yaml file.
    table.update_metadata_from_yaml(METADATA_PATH, DATASET_SHORT_NAME)

    return table


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load BP's statistical review dataset.
    ds_bp = catalog.Dataset(BP_DATASET_PATH)
    # Select main table.
    table_bp = ds_bp["statistical_review"]
    # Create a convenient dataframe.
    df_bp = pd.DataFrame(table_bp)

    # Idem for Ember's combined (global & european) electricity review.
    ds_ember = catalog.Dataset(EMBER_DATASET_PATH)
    table_ember = ds_ember["combined_electricity_review"]
    df_ember = pd.DataFrame(table_ember)

    #
    # Process data.
    #
    # Prepare BP and Ember data.
    df_bp = process_bp_data(table_bp=table_bp)
    df_ember = process_ember_data(table_ember=table_ember)

    # Combine both tables, giving priority to Ember data (on overlapping values).
    combined = combine_two_overlapping_dataframes(df1=df_ember, df2=df_bp, index_columns=["country", "year"])

    # Add carbon intensities.
    # There is already a variable for this in the Ember dataset, but now that we have combined
    # BP and Ember data, intensities should be recalculated for consistency.
    combined["co2_intensity__gco2_kwh"] = (combined["total_emissions__mtco2"] * MT_TO_G) / (
        combined["total_generation__twh"] * TWH_TO_KWH
    )

    # Add per capita variables.
    combined = add_per_capita_variables(combined=combined)

    # Add "share" variables.
    combined = add_share_variables(combined=combined)

    # Prepare output table.
    table = prepare_output_table(combined=combined)

    #
    # Save outputs.
    #
    ds_garden = catalog.Dataset.create_empty(dest_dir)
    # Import metadata from the metadata yaml file.
    ds_garden.metadata.update_from_yaml(METADATA_PATH, if_source_exists="replace")
    # Create dataset.
    ds_garden.save()

    # Add combined tables to the new dataset.
    ds_garden.add(table)

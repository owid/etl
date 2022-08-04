"""Garden step that combines BP's statistical review with Ember's combined (Global & European) Electricity Review
to create the Electricity Mix (BP & Ember) dataset.

"""

import pandas as pd
from owid import catalog

from etl.paths import DATA_DIR
from shared import CURRENT_DIR, add_population, combine_two_overlapping_dataframes

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
    aggregates = {
        "fossil_generation__twh": {
            "columns": [
                "oil_generation__twh",
                "coal_generation__twh",
                "gas_generation__twh",
            ],
            "metadata": catalog.VariableMeta(
                title="Generation - Fossil fuels (TWh)",
                unit="terawatt-hours",
                short_unit="TWh",
                display={"name": "Fossil fuels"},
            ),
        },
        "renewable_generation__twh": {
            "columns": [
                "hydro_generation__twh",
                "solar_generation__twh",
                "wind_generation__twh",
                "other_renewables_including_bioenergy_generation__twh",
            ],
            "metadata": catalog.VariableMeta(
                title="Generation - Renewables (TWh)",
                unit="terawatt-hours",
                short_unit="TWh",
                display={"name": "Renewables"},
            ),
        },
        "low_carbon_generation__twh": {
            "columns": ["renewable_generation__twh", "nuclear_generation__twh"],
            "metadata": catalog.VariableMeta(
                title="Generation - Low-carbon sources (TWh)",
                unit="terawatt-hours",
                short_unit="TWh",
                display={"name": "Low-carbon sources"},
            ),
        },
    }

    # Create new columns, by adding up other columns (and allowing for only one nan in each sum).
    for new_column, aggregate in aggregates.items():
        table_bp[new_column] = pd.DataFrame(table_bp)[aggregate["columns"]].sum(
            axis=1, min_count=len(aggregate["columns"]) - 1
        )
        # Create metadata for this new variable.
        table_bp[new_column].metadata = aggregate["metadata"]

    # Prepare data in a dataframe with a dummy index.
    df_bp = pd.DataFrame(table_bp).reset_index()

    return df_bp


def process_ember_data(table_ember: catalog.Table) -> pd.DataFrame:
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

    # In BP data, there is a variable "Geo Biomass Other", which combines all other renewables.
    # In Ember data, "other rewenables" excludes bioenergy.
    # To be able to combine both datasets, create a new variable for generation of other renewables including bioenergy.
    table_ember["other_renewables_including_bioenergy_generation__twh"] = (
        pd.DataFrame(table_ember)[
            "other_renewables_excluding_bioenergy_generation__twh"
        ]
        + table_ember["bioenergy_generation__twh"]
    )

    # Add variable metadata to this new variable.
    table_ember[
        "other_renewables_including_bioenergy_generation__twh"
    ].metadata = catalog.VariableMeta(
        title="Generation - Other renewables including bioenergy (TWh)",
        unit="terawatt-hours",
        short_unit="TWh",
        display={"name": "Other renewables including bioenergy"},
    )

    # Prepare data in a dataframe with a dummy index.
    df_ember = pd.DataFrame(table_ember).reset_index()

    return df_ember


def add_per_capita_variables(combined: pd.DataFrame) -> pd.DataFrame:
    combined = combined.copy()

    # Variables to make per capita (new variable names will be 'per_capita_' followed by the original variable name).
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
        assert (
            "twh" in variable
        ), f"Variables are assumed to be in TWh, but {variable} is not."
        new_column = "per_capita_" + variable.replace("__twh", "__kwh")
        combined[new_column] = combined[variable] * TWH_TO_KWH / combined["population"]

    return combined


def add_share_variables(combined: pd.DataFrame) -> pd.DataFrame:
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
        combined[new_column] = (
            100 * combined[variable] / combined["total_generation__twh"]
        )

    # Calculate the percentage of electricity as a share of primary energy.
    combined["total_electricity_share_of_primary_energy__pct"] = (
        100
        * combined["total_generation__twh"]
        / combined["primary_energy_consumption__twh"]
    )

    # Calculate the percentage of electricity demand that is imported.
    combined["net_imports_share_of_demand__pct"] = (
        100 * combined["total_net_imports__twh"] / combined["total_demand__twh"]
    )

    # Sanity check.
    error = "Total electricity share does not add up to 100%."
    assert all(
        abs(combined["total_share_of_electricity__pct"].dropna() - 100) < 0.01
    ), error

    # Remove unnecessary columns.
    combined = combined.drop(columns=["total_share_of_electricity__pct"])

    return combined


def prepare_output_table(combined: pd.DataFrame) -> catalog.Table:
    # Sort rows and columns conveniently and set an index.
    combined = combined[sorted(combined.columns)]
    combined = combined.set_index(
        ["country", "year"], verify_integrity=True
    ).sort_index()

    # Convert dataframe into a table (with no metadata).
    table = catalog.Table(combined)

    # Load metadata from yaml file.
    table.update_metadata_from_yaml(METADATA_PATH, DATASET_SHORT_NAME)

    return table


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Read BP's statistical review.
    ds_bp = catalog.Dataset(BP_DATASET_PATH)
    table_bp = ds_bp["statistical_review"]

    # Read Ember's combined (global & european) electricity review.
    ds_ember = catalog.Dataset(EMBER_DATASET_PATH)
    table_ember = ds_ember["combined_electricity_review"]

    #
    # Process data.
    #
    # Prepare BP and Ember data.
    df_bp = process_bp_data(table_bp=table_bp)
    df_ember = process_ember_data(table_ember=table_ember)

    # Combine both tables, giving priority to BP data (on overlapping values).
    combined = combine_two_overlapping_dataframes(
        df1=df_bp, df2=df_ember, index_columns=["country", "year"]
    )

    # Add carbon intensities.
    # There is already a variable for this in the Ember dataset, but now that we have combined
    # BP and Ember data, intensities should be recalculated for consistency.
    combined["co2_intensity__gco2_kwh"] = (
        combined["total_emissions__mtco2"] * MT_TO_G
    ) / (combined["total_generation__twh"] * TWH_TO_KWH)

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
    ds_garden.metadata.update_from_yaml(METADATA_PATH)
    # Create dataset.
    ds_garden.save()

    # Add combined tables to the new dataset.
    # Import metadata from meadow and update attributes that have changed.
    ds_garden.add(table)

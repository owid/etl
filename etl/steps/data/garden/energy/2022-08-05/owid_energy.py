"""Garden step that combines various datasets related to energy and produces the OWID Energy dataset (2022).

Datasets combined:
* Statistical Review of World Energy - BP (2022).
* Energy mix from BP.
* Fossil fuel production (BP & Shift, 2022).
* Primary energy consumption (BP & EIA, 2022).
* Electricity mix (BP & Ember, 2022).

"""

import pandas as pd
from owid import catalog

from etl.paths import DATA_DIR
from shared import CURRENT_DIR

# Details for dataset to export.
DATASET_SHORT_NAME = "owid_energy"
METADATA_PATH = CURRENT_DIR / f"{DATASET_SHORT_NAME}.meta.yml"
# Details for datasets to import.
STATISTICAL_REVIEW_DATASET_PATH = DATA_DIR / "garden/bp/2022-07-14/statistical_review"
ENERGY_MIX_DATASET_PATH = DATA_DIR / "garden/bp/2022-07-14/energy_mix"
FOSSIL_FUEL_PRODUCTION_DATASET_PATH = DATA_DIR / "garden/energy/2022-07-20/fossil_fuel_production"
PRIMARY_ENERGY_CONSUMPTION_DATASET_PATH = DATA_DIR / "garden/energy/2022-07-29/primary_energy_consumption"
ELECTRICITY_MIX_DATASET_PATH = DATA_DIR / "garden/energy/2022-08-03/electricity_mix"


def process_bp_data(table_bp: catalog.Table) -> pd.DataFrame:
    """Load necessary columns from BP's Statistical Review dataset.

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
        # TODO: Add required columns.
    }
    table_bp = table_bp[list(columns)].rename(columns=columns, errors="raise")

    # Prepare data in a dataframe with a dummy index.
    df_bp = pd.DataFrame(table_bp).reset_index()

    return df_bp


def combine_all_data(df_statistical_review: pd.DataFrame, df_energy_mix: pd.DataFrame, df_fossil_fuels: pd.DataFrame,
                     df_primary_energy: pd.DataFrame, df_electricity_mix: pd.DataFrame) -> pd.DataFrame:
    combined = pd.DataFrame()

    return combined


def prepare_output_table(combined: pd.DataFrame) -> catalog.Table:
    """Convert the combined dataframe into a table with the appropriate metadata and variables metadata.

    Parameters
    ----------
    combined : pd.DataFrame
        Combined data from all tables.

    Returns
    -------
    table : catalog.Table
        Original data in a table format with metadata.

    """
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
    # Read all required datasets.
    ds_statistical_review = catalog.Dataset(STATISTICAL_REVIEW_DATASET_PATH)
    ds_energy_mix = catalog.Dataset(ENERGY_MIX_DATASET_PATH)
    ds_fossil_fuels = catalog.Dataset(FOSSIL_FUEL_PRODUCTION_DATASET_PATH)
    ds_primary_energy = catalog.Dataset(PRIMARY_ENERGY_CONSUMPTION_DATASET_PATH)
    ds_electricity_mix = catalog.Dataset(ELECTRICITY_MIX_DATASET_PATH)

    # Gather all data from all required tables in datasets.
    df_statistical_review = pd.DataFrame(ds_statistical_review[ds_statistical_review.table_names[0]]).reset_index()
    df_energy_mix = pd.DataFrame(ds_energy_mix[ds_electricity_mix.table_names[0]]).reset_index()
    df_fossil_fuels = pd.DataFrame(ds_fossil_fuels[ds_fossil_fuels.table_names[0]]).reset_index()
    df_primary_energy = pd.DataFrame(ds_primary_energy[ds_primary_energy.table_names[0]]).reset_index()
    df_electricity_mix = pd.DataFrame(ds_electricity_mix[ds_electricity_mix.table_names[0]]).reset_index()

    #
    # Process data.
    #
    # Combine all tables.
    combined = combine_all_data(
        df_statistical_review=df_statistical_review, df_energy_mix=df_energy_mix, df_fossil_fuels=df_fossil_fuels,
        df_primary_energy=df_primary_energy, df_electricity_mix=df_electricity_mix)

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
    ds_garden.add(table)

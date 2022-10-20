"""Create a dataset of renewable electricity capacity using IRENA's Renewable electricity capacity and generation dataset.

"""

import json
from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.datautils import geo

from etl.paths import DATA_DIR, STEP_DIR

# Details of inputs.
MEADOW_DATASET_NAME = "renewable_electricity_capacity_and_generation"
MEADOW_VERSION = "2022-10-20"
MEADOW_DATASET_PATH = DATA_DIR / f"meadow/irena/{MEADOW_VERSION}/{MEADOW_DATASET_NAME}"
# Details of outputs.
DATASET_NAME = "renewable_electricity_capacity"
TABLE_NAME = DATASET_NAME
VERSION = MEADOW_VERSION
COUNTRIES_PATH = STEP_DIR / f"data/garden/irena/{VERSION}/{DATASET_NAME}.countries.json"
METADATA_PATH = STEP_DIR / f"data/garden/irena/{VERSION}/{DATASET_NAME}.meta.yml"


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from Meadow.
    ds_meadow = Dataset(MEADOW_DATASET_PATH)
    # Load main table from dataset.
    tb_meadow = ds_meadow[ds_meadow.table_names[0]]
    # Create a dataframe out of the main table.
    df = pd.DataFrame(tb_meadow).reset_index()

    #
    # Process data.
    #
    # Harmonize country names.
    df = geo.harmonize_countries(df=df, countries_file=COUNTRIES_PATH)

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["technology", "country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)
    
    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata
    # Ensure all columns are snake, lower case.
    tb_garden = underscore_table(Table(df))
    tb_garden.short_name = TABLE_NAME
    # Load dataset's metadata from yaml file.
    ds_garden.metadata.update_from_yaml(METADATA_PATH)
    # Load main table's metadata from yaml file.
    tb_garden.update_metadata_from_yaml(METADATA_PATH, TABLE_NAME)
    # Add table to dataset and save dataset.
    ds_garden.add(tb_garden)
    ds_garden.save()

"""Create a dataset of renewable electricity capacity using IRENA's Renewable Electricity Capacity and Generation.

"""

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table

from etl.data_helpers import geo
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

    # Reshape dataframe to have each technology as a separate column, and sort conveniently.
    df = (
        df.pivot(index=["country", "year"], columns=["technology"], values="capacity")
        .rename_axis(None, axis=1)
        .sort_index()
        .sort_index(axis=1)
    )

    # For convenience, remove parentheses from column names.
    df = df.rename(columns={column: column.replace("(", "").replace(")", "") for column in df.columns})

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

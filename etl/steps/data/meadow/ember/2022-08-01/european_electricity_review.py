"""Meadow step for European Electricity Review (Ember, 2022).

"""

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict
from zipfile import ZipFile

import pandas as pd
from owid import catalog
from owid.walden import Catalog as WaldenCatalog
from shared import VERSION, log

from etl.steps.data.converters import convert_walden_metadata

# Details of dataset to export.
NAMESPACE = "ember"
DATASET_SHORT_NAME = "european_electricity_review"
# Details of walden dataset to import.
WALDEN_VERSION = "2022-02-01"
# Beginning of the names of csv files inside the raw zip file in walden, and their extension.
FILES_NAME_START = "EER_2022_"
FILES_EXTENSION = ".csv"

TABLE_INDEXES = {
    "country_overview": ["country_name", "year"],
    "emissions": ["country_name", "year"],
    "generation": ["country_name", "year", "fuel_code"],
    "net_flows": ["source_country_code", "target_country_code", "year"],
}

TABLE_NAMES = {
    "country_overview": "Country overview",
    "emissions": "Emissions",
    "generation": "Generation",
    "net_flows": "Net flows",
}


def load_dataframes_from_compressed_folder(
    zip_file_path: str,
) -> Dict[str, pd.DataFrame]:
    # Initialise dictionary that will contain the dataframes.
    dfs = {}
    with TemporaryDirectory() as _temp_folder:
        with ZipFile(zip_file_path) as _zip_folder:
            for file in _zip_folder.namelist():
                temp_file = Path(_temp_folder) / file
                # Check all files in the folder (there are some hidden mac files that start with "__"), and keep only
                # files of the required extension.
                if file.endswith(FILES_EXTENSION) and not file.startswith("__"):
                    df_name = file.replace(FILES_EXTENSION, "").replace(FILES_NAME_START, "")
                    # Extract file, read it as a dataframe, and store it in dictionary.
                    _zip_folder.extract(file, path=_temp_folder)
                    dfs[df_name] = pd.read_csv(temp_file)

    return dfs


def create_tables(dfs: Dict[str, pd.DataFrame]) -> Dict[str, catalog.Table]:
    tables = {}
    for df_name in list(dfs):
        df = dfs[df_name].set_index(TABLE_INDEXES[df_name], verify_integrity=True)
        table_name = TABLE_NAMES[df_name]
        tables[table_name] = catalog.Table(df)

    return tables


def run(dest_dir: str) -> None:
    log.info(f"{DATASET_SHORT_NAME}.start")

    # Retrieve raw data from walden.
    walden_ds = WaldenCatalog().find_one(namespace=NAMESPACE, short_name=DATASET_SHORT_NAME, version=WALDEN_VERSION)
    local_file = walden_ds.ensure_downloaded()

    # Original zip file contains various csv files.
    # Create a dictionary that contains all dataframes.
    dfs = load_dataframes_from_compressed_folder(zip_file_path=local_file)

    # Convert each dataframe into a table, and ensure the indexes are consistent.
    tables = create_tables(dfs)

    # Create new dataset, reuse walden metadata, and update metadata.
    ds = catalog.Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = VERSION
    ds.save()

    # Create tables.
    for table_name in tables:
        table = tables[table_name]
        table.metadata.short_name = catalog.utils.underscore(table_name)
        table.metadata.title = table_name
        table.metadata.description = walden_ds.description
        # Underscore all table columns.
        table = catalog.utils.underscore_table(table)
        # Add table to dataset.
        ds.add(table)

    log.info(f"{DATASET_SHORT_NAME}.end")

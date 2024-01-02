"""Meadow step for European Electricity Review (Ember, 2022).

"""

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict
from zipfile import ZipFile

import owid.catalog.processing as pr
import pandas as pd
from owid import catalog

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get naming conventions.
paths = PathFinder(__file__)


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


def load_tables_from_compressed_folder(
    snap: Snapshot,
) -> Dict[str, pd.DataFrame]:
    # Beginning of the names of csv files inside the raw zip file in walden, and their extension.
    files_name_start = "EER_2022_"
    files_extension = ".csv"

    # Initialise dictionary that will contain the tables.
    tables = {}
    with TemporaryDirectory() as _temp_folder:
        with ZipFile(snap.path) as _zip_folder:
            for file in _zip_folder.namelist():
                temp_file = Path(_temp_folder) / file
                # Check all files in the folder (there are some hidden mac files that start with "__"), and keep only
                # files of the required extension.
                if file.endswith(files_extension) and not file.startswith("__"):
                    table_name = file.replace(files_extension, "").replace(files_name_start, "")
                    # Extract file, read it as a dataframe, and store it in dictionary.
                    _zip_folder.extract(file, path=_temp_folder)
                    new_table = pr.read_csv(temp_file, metadata=snap.to_table_metadata(), origin=snap.metadata.origin)
                    new_table.metadata.short_name = table_name
                    tables[table_name] = new_table

    return tables


def create_tables(dfs: Dict[str, pd.DataFrame]) -> Dict[str, catalog.Table]:
    tables = {}
    for df_name in list(dfs):
        df = dfs[df_name].set_index(TABLE_INDEXES[df_name], verify_integrity=True)
        table_name = TABLE_NAMES[df_name]
        tables[table_name] = catalog.Table(df)

    return tables


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("european_electricity_review.zip")
    tables = load_tables_from_compressed_folder(snap)

    #
    # Process data.
    #
    for table_name in tables:
        # Add a title to each table.
        tables[table_name].metadata.title = TABLE_NAMES[table_name]
        # Set an appropriate index to each table and sort them conveniently.
        tables[table_name] = (
            tables[table_name]
            .set_index(TABLE_INDEXES[table_name], verify_integrity=True)
            .sort_index()
            .sort_index(axis=1)
        )

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(
        dest_dir, tables=tables.values(), default_metadata=snap.metadata, check_variables_metadata=True
    )
    ds_meadow.save()

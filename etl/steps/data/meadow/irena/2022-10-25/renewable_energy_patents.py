import pandas as pd
from owid.catalog import Dataset, Table, TableMeta

from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

# Details for input datasets.
WALDEN_VERSION = "2022-10-25"
WALDEN_DATASET_NAME = "renewable_energy_patents"
# Details for output dataset.
VERSION = WALDEN_VERSION

# Columns to use from raw data and how to rename them.
COLUMNS = {
    "Country/area": "country",
    "Year": "year",
    "Sector": "sector",
    "Technology": "technology",
    "Sub Technology": "sub_technology",
    "Number of Patents": "patents",
}


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load raw data from Walden.
    snap = Snapshot(f"irena/{WALDEN_VERSION}/{WALDEN_DATASET_NAME}.csv")
    local_file = str(snap.path)
    df = pd.read_csv(local_file)

    #
    # Process data.
    #
    # Select and rename columns conveniently.
    df = df[list(COLUMNS)].rename(columns=COLUMNS)

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["country", "year", "sector", "technology", "sub_technology"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create new dataset and reuse metadata form Walden.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)
    ds.metadata.version = VERSION

    # Create table with metadata from dataframe
    table_metadata = TableMeta(
        short_name=snap.metadata.short_name, title=snap.metadata.name, description=snap.metadata.description
    )
    tb = Table(df, metadata=table_metadata)

    # Add table to new dataset and save dataset.
    ds.add(tb)
    ds.save()

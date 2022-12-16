import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table

from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

# Details for dataset to be created.
VERSION = "2022-09-14"
DATASET_NAME = "emissions_weighted_carbon_price__coverage"
# Details of dataset in walden.
WALDEN_VERSION = "2022-09-14"
WALDEN_DATASET_NAME = DATASET_NAME


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load data from walden.
    snap = Snapshot(f"rff/{WALDEN_VERSION}/{WALDEN_DATASET_NAME}.csv")
    df = pd.read_csv(snap.path, dtype=object)

    #
    # Process data.
    #
    # Sanity check.
    error = "There should be one row per jurisdiction-year."
    assert df[df.duplicated(subset=["jurisdiction", "year"])].empty, error
    error = "There should not be any row that only has nan data."
    assert df[df.drop(columns=["jurisdiction", "year"]).isnull().all(axis=1)].empty, error

    # Set an index and sort conveniently.
    df = df.set_index(["jurisdiction", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create new dataset with metadata from walden.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)
    ds.metadata.version = VERSION
    # Create new table with metadata.
    table_metadata = TableMeta(
        short_name=snap.metadata.short_name, title=snap.metadata.name, description=snap.metadata.description
    )
    tb = Table(df, metadata=table_metadata)
    # Prepare table and add it to the dataset.
    tb = underscore_table(tb)
    ds.add(tb)
    # Save the dataset.
    ds.save()

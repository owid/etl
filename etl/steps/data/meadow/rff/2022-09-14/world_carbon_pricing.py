import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table

from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

# Version for dataset to be created.
VERSION = "2022-09-14"
# Version of dataset in walden.
WALDEN_VERSION = "2022-09-14"

# Columns to select and rename in ipcc column names.
IPCC_COLUMNS = {
    "IPCC_CODE": "ipcc_code",
    "FULLNAME": "sector_name",
}


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load data from walden.
    snap = Snapshot(f"rff/{WALDEN_VERSION}/world_carbon_pricing.csv")
    df = pd.read_csv(snap.path, dtype=object)
    # Load IPCC codes from walden.
    walden_ipcc_ds = Snapshot(f"rff/{WALDEN_VERSION}/ipcc_codes.csv")
    ipcc_codes = pd.read_csv(walden_ipcc_ds.path, dtype=object)

    #
    # Process data.
    #
    # Prepare IPCC codes dataframe.
    ipcc_codes = ipcc_codes[list(IPCC_COLUMNS)].rename(columns=IPCC_COLUMNS)
    # Sanity check.
    error = "IPCC codes found in data that are missing in IPCC codes file."
    assert set(df["ipcc_code"]) <= set(ipcc_codes["ipcc_code"]), error
    # Add sector names to data, mapping IPCC codes.
    df = pd.merge(df, ipcc_codes, on="ipcc_code", how="left")

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

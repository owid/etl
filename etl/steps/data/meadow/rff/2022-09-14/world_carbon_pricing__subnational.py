import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog

from etl.steps.data.converters import convert_walden_metadata

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
    walden_ds = WaldenCatalog().find_one(
        namespace="rff", short_name="world_carbon_pricing__subnational", version=WALDEN_VERSION
    )
    df = pd.read_csv(walden_ds.ensure_downloaded(), dtype=object)
    # Load IPCC codes from walden.
    walden_ipcc_ds = WaldenCatalog().find_one(namespace="rff", short_name="ipcc_codes", version=WALDEN_VERSION)
    ipcc_codes = pd.read_csv(walden_ipcc_ds.ensure_downloaded(), dtype=object)

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
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = VERSION
    # Create new table with metadata.
    table_metadata = TableMeta(short_name=walden_ds.short_name, title=walden_ds.name, description=walden_ds.description)
    tb = Table(df, metadata=table_metadata)
    # Prepare table and add it to the dataset.
    tb = underscore_table(tb)
    ds.add(tb)
    # Save the dataset.
    ds.save()

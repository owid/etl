import gzip
import json

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog
from shared import NAMESPACE, VERSION

from etl.steps.data.converters import convert_walden_metadata

DATASET_SHORT_NAME = "ghg_emissions_by_sector"
DATASET_TITLE = "Greenhouse gas emissions by sector"
WALDEN_SHORT_NAME = "cait_ghg_emissions"
WALDEN_VERSION = "2022-08-10"


def load_data(local_file: str) -> pd.DataFrame:
    """Create a dataframe out of the raw data.

    Parameters
    ----------
    local_file : str
        Path to local file of raw data.

    Returns
    -------
    df : pd.DataFrame
        Raw data in dataframe format.

    """
    with gzip.open(local_file) as _file:
        data = json.loads(_file.read())

    df = pd.DataFrame.from_dict(data)

    return df


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare raw data in a more convenient format.

    Parameters
    ----------
    df : pd.DataFrame
        Original raw data as a dataframe.

    Returns
    -------
    df : pd.DataFrame
        Original data in a more convenient format.

    """
    # Extract data from column "emissions", which is given as a list of dictionaries with year and value.
    df = df.explode("emissions").reset_index(drop=True)
    df["year"] = [emissions["year"] for emissions in df["emissions"]]
    df["value"] = [emissions["value"] for emissions in df["emissions"]]
    df = df.drop(columns="emissions")

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["country", "year", "gas", "sector", "data_source"], verify_integrity=True).sort_index()

    return df


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Retrieve raw data from walden.
    walden_ds = WaldenCatalog().find_one(namespace=NAMESPACE, short_name=WALDEN_SHORT_NAME, version=WALDEN_VERSION)
    local_file = walden_ds.ensure_downloaded()

    # Create a dataframe from compressed file.
    df = load_data(local_file=local_file)

    #
    # Process data.
    #
    # Prepare data in a convenient format.
    df = prepare_data(df=df)

    #
    # Save outputs.
    #
    # Create new dataset, reuse walden metadata, and update metadata.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.short_name = DATASET_SHORT_NAME
    ds.metadata.title = DATASET_TITLE
    ds.metadata.version = VERSION
    ds.save()

    # Create table with metadata from walden.
    tb_metadata = TableMeta(
        short_name=DATASET_SHORT_NAME,
        title=DATASET_TITLE,
        description=walden_ds.description,
    )
    tb = Table(df, metadata=tb_metadata)
    # Underscore all table columns.
    tb = underscore_table(tb)
    # Add table to dataset.
    ds.add(tb)

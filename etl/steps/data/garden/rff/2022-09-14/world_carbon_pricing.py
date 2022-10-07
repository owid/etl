import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.datautils import geo
from shared import CURRENT_DIR

from etl.helpers import Names

DATASET_SHORT_NAME = "world_carbon_pricing"
MEADOW_TABLE_NAME = DATASET_SHORT_NAME
GARDEN_MAIN_TABLE_NAME = MEADOW_TABLE_NAME
GARDEN_ANY_SECTOR_TABLE_NAME = "world_carbon_pricing_any_sector"
# Get naming convention for this dataset.
N = Names(str(CURRENT_DIR / DATASET_SHORT_NAME))

# Columns to keep from raw dataset and how to rename them.
COLUMNS = {
    "jurisdiction": "country",
    "year": "year",
    "ipcc_code": "ipcc_code",
    "product": "product",
    "sector_name": "sector_name",
    "tax": "tax",
    "ets": "ets",
    "tax_rate_excl_ex_clcu": "tax_rate_gross",
    "tax_rate_incl_ex_clcu": "tax_rate_net",
    "ets_price": "ets_price",
}

# Columns to use as index.
INDEX_COLUMNS = ["country", "year", "ipcc_code", "product"]


def sanity_checks(df: pd.DataFrame) -> None:
    """Sanity checks on the raw data.

    Parameters
    ----------
    df : pd.DataFrame
        Raw data from meadow.

    """
    column_checks = (
        df.groupby("jurisdiction")
        .agg(
            {
                # Columns 'tax' and 'ets' must contain only 0 and/or 1.
                "tax": lambda x: set(x) <= {0, 1},
                "ets": lambda x: set(x) <= {0, 1},
            }
        )
        .all()
    )
    # Column tax_id either is nan or has one value, which is the iso code of the country followed by "tax"
    # (e.g. 'aus_tax'). However there is at least one exception, Norway has 'nor_tax_I', so maybe the data is
    # expected to have more than one 'tax_id'.

    # Similarly, 'ets_id' is either nan, or usually just one value, e.g. "eu_ets" for EU countries, or "nzl_ets",
    # "mex_ets", etc. However for the UK there are two, namely {'gbr_ets', 'eu_ets'}.

    error = f"Unexpected content in columns {column_checks[~column_checks].index.tolist()}."
    assert column_checks.all(), error


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare data.

    Parameters
    ----------
    df : pd.DataFrame
        Raw data.

    Returns
    -------
    df : pd.DataFrame
        Clean data.

    """
    df = df.copy()

    # Select and rename columns.
    df = df[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Column 'product' has many nans. Convert them into empty strings.
    df["product"] = df["product"].cat.add_categories("").fillna("")

    return df


def create_table_for_any_sector(tb: Table) -> Table:
    # Create a simplified table that gives, for each country and year, whether the country has any sector(-fuel)
    # that is covered by at least one tax instrument. And idem for ets.
    tb_any_sector = (
        tb.reset_index()
        .groupby(["country", "year"], observed=True)
        .agg({"ets": lambda x: min(x.sum(), 1), "tax": lambda x: min(x.sum(), 1)})
        .astype(int)
        .reset_index()
    )

    # Set an appropriate index and sort conveniently.
    tb_any_sector = tb_any_sector.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)
    # Create table for simplified data.
    tb_any_sector = underscore_table(tb_any_sector).reset_index()

    return tb_any_sector


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Read dataset from meadow.
    ds_meadow = N.meadow_dataset
    # Get table from dataset.
    tb_meadow = ds_meadow[MEADOW_TABLE_NAME]
    # Construct a dataframe from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    # Sanity checks on raw data.
    sanity_checks(df=df)

    # Prepare data.
    df = prepare_data(df=df)

    # Harmonize country names.
    df = geo.harmonize_countries(df=df, countries_file=str(N.country_mapping_path), warn_on_unused_countries=False)

    # Set an appropriate index and sort conveniently.
    df = df.set_index(INDEX_COLUMNS, verify_integrity=True).sort_index().sort_index(axis=1)

    # Create main table.
    tb_garden = underscore_table(Table(df))

    # Create a simplified table for "any sector".
    tb_any_sector = create_table_for_any_sector(tb=tb_garden)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = Dataset.create_empty(dest_dir)
    # Fetch metadata from meadow step (if any).
    ds_garden.metadata = ds_meadow.metadata
    # Update dataset metadata using metadata yaml file.
    ds_garden.metadata.update_from_yaml(N.metadata_path, if_source_exists="replace")
    # Update main table metadata using metadata yaml file.
    tb_garden.update_metadata_from_yaml(N.metadata_path, GARDEN_MAIN_TABLE_NAME)
    # Update simplified table metadata using metadata yaml file.
    tb_any_sector.update_metadata_from_yaml(N.metadata_path, GARDEN_ANY_SECTOR_TABLE_NAME)
    # Add tables to dataset.
    ds_garden.add(tb_garden)
    ds_garden.add(tb_any_sector)
    # Save dataset.
    ds_garden.save()

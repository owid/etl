import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.datautils import geo

from etl.helpers import Names
from shared import CURRENT_DIR

# naming conventions
N = Names(CURRENT_DIR / "world_carbon_pricing.py")

# Columns to keep from raw dataset and how to rename them.
COLUMNS = {
    "jurisdiction": "country",
    "year": "year",
    "ipcc_code": "ipcc_code",
    "product": "product",
    "tax": "tax",
    "ets": "ets",
    "tax_rate_excl_ex_clcu": "tax_rate_excl_ex_clcu",
    "tax_rate_incl_ex_clcu": "tax_rate_incl_ex_clcu",
    "ets_price": "ets_price",
}

# Columns to use as index.
INDEX_COLUMNS = ["country", "year", "ipcc_code", "product"]

# Minimum and maximum expected years (only needed to assert that each country has data for all years).
MIN_YEAR = 1989
MAX_YEAR = 2022


def sanity_checks(df: pd.DataFrame) -> None:
    expected_years = set(np.arange(MIN_YEAR, MAX_YEAR + 1))
    column_checks = df.groupby("jurisdiction").agg({
        # Column year always has all years for each country (which is actually unnecessary, since most rows are empty).
        "year": lambda x: set(x) == expected_years,
        # Columns 'tax' and 'ets' must contain only 0 and/or 1.
        "tax": lambda x: set(x) <= {0, 1},
        "ets": lambda x: set(x) <= {0, 1},
    }).all()
    # Column tax_id either is nan or has one value, which is the iso code of the country followed by "tax"
    # (e.g. 'aus_tax'). However there is at least one exception, Norway has 'nor_tax_I', so maybe the data is
    # expected to have more than one 'tax_id'.

    # Similarly, 'ets_id' is either nan, or usually just one value, e.g. "eu_ets" for EU countries, or "nzl_ets",
    # "mex_ets", etc. However for the UK there are two, namely {'gbr_ets', 'eu_ets'}.
    
    error = f"Unexpected content in columns {column_checks[~column_checks].index.tolist()}."
    assert column_checks.all(), error


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Select and rename columns.
    df = df[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Column 'product' has many nans. Convert them into empty strings.
    df['product'] = df['product'].cat.add_categories("").fillna("")

    # Columns 'tax' and 'ets' are either 0 or 1, while all other columns are either nan or some value.
    # Convert zeros in 'tax' and 'ets' to nan, and then drop all rows where all data columns are nan.
    # This way we get rid of all unnecessary rows where there is no data.
    df["tax"] = df["tax"].replace(0, np.nan)
    df["ets"] = df["ets"].replace(0, np.nan)

    # Remove rows where all data columns are nan.
    df = df.dropna(subset=[column for column in df.columns if column not in INDEX_COLUMNS], how="all").\
        reset_index(drop=True)
   
    # Columns 'tax' and 'ets' were converted to float (because we introduced nans).
    # Now that nans have been removed, make them integer again.
    df["tax"] = df["tax"].fillna(0)
    df["ets"] = df["ets"].fillna(0)
    df = df.astype({"tax": int, "ets": int})

    return df


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Read dataset from meadow.
    ds_meadow = N.meadow_dataset
    # Get table from dataset.
    tb_meadow = ds_meadow["world_carbon_pricing"]
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

    #
    # Save outputs.
    #
    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    tb_garden = underscore_table(Table(df))
    tb_garden.metadata = tb_meadow.metadata
    for col in tb_garden.columns:
        tb_garden[col].metadata = tb_meadow[col].metadata

    ds_garden.metadata.update_from_yaml(N.metadata_path)
    tb_garden.update_metadata_from_yaml(N.metadata_path, "world_carbon_pricing")

    ds_garden.add(tb_garden.reset_index())

    ds_garden.save()

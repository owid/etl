import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.datautils import geo
from structlog import get_logger
from tqdm.auto import tqdm

from etl.helpers import Names

log = get_logger()

# naming conventions
N = Names(__file__)

# Minimum and maximum expected years (only needed to assert that each country has data for all years).
MIN_YEAR = 1989
MAX_YEAR = 2022

# Columns to keep from raw dataset and how to rename them.
COLUMNS = {
    'jurisdiction': 'country',
    'year': 'year',
    'ipcc_code': 'ipcc_code',
    'product': 'product',
    'tax': 'tax',
    'ets': 'ets',
    'tax_rate_excl_ex_clcu': 'tax_rate_excl_ex_clcu',
    'tax_rate_incl_ex_clcu': 'tax_rate_incl_ex_clcu',
    'ets_price': 'ets_price',
}

# Types of columns to keep.
DTYPES = {
        'country': str,
        'year': int,
        'ipcc_code': str,
        'product': str,
        'tax': int,
        'ets': int,
        'tax_rate_excl_ex_clcu': float,
        'tax_rate_incl_ex_clcu': float,
        'ets_price': float,
    }


def sanity_checks(data):
    # TODO: Simplify the following by grouping by country and doing different aggregates.
    for country in tqdm(data["country"].unique()):
        df = data[data["country"] == country].reset_index(drop=True)

        error = f"All years between {MIN_YEAR} and {MAX_YEAR} should be covered in the data."
        assert set(df["year"]) == set(np.arange(MIN_YEAR, MAX_YEAR + 1)), error

        error = "'tax' column should contain either 0 or 1 or both."
        assert set(df["tax"]) <= set([0, 1]), error

        error = "'ets' column should contain either 0 or 1 or both."
        assert set(df["ets"]) <= set([0, 1]), error

        # Column tax_id either is nan or has one value, which is the iso code of the country followed by "tax"
        # (e.g. 'aus_tax'). However there is at least one exception, Norway has 'nor_tax_I', so maybe the data is
        # expected to have more than one 'tax_id'.

        # Similarly, 'ets_id' is either nan, or usually just one value, e.g. "eu_ets" for EU countries, or "nzl_ets",
        # "mex_ets"... However for the UK there are two, namely {'gbr_ets', 'eu_ets'}.


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Columns 'tax' and 'ets' are either 0 or 1, while all other columns are either nan or some value.
    # Convert zeros in 'tax' and 'ets' to nan, and then drop all rows where all data columns are nan.
    # This way we get rid of all unnecessary rows where there is no data.
    df["tax"] = df["tax"].replace(0, np.nan)
    df["ets"] = df["ets"].replace(0, np.nan)

    index_columns = ["country", "year", "ipcc_code", "product"]
    df = df.set_index(index_columns).dropna(how="all")

    return df


def run(dest_dir: str) -> None:
    log.info("world_carbon_pricing.start")

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
    # Prepare data in a convenient format.
    df = df.rename(columns=COLUMNS, errors="raise").astype(DTYPES)

    # Sanity checks on raw data.
    sanity_checks(data=df)

    # Harmonize country names.
    df = geo.harmonize_countries(df=df, countries_file=str(N.country_mapping_path))

    # Prepare data.
    df = prepare_data(df=df)

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
    
    ds_garden.add(tb_garden)
    ds_garden.save()

    log.info("world_carbon_pricing.end")

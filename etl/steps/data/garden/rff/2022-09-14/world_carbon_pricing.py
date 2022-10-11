from typing import Dict, List, cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.datautils import dataframes, geo, io
from shared import CURRENT_DIR

from etl.helpers import Names
from etl.paths import DATA_DIR, STEP_DIR

# Details of the input dataset.
MEADOW_DATASET_NAME = "world_carbon_pricing"
MEADOW_VERSION = "2022-09-14"
MEADOW_MAIN_DATASET_PATH = DATA_DIR / f"meadow/rff/{MEADOW_VERSION}/{MEADOW_DATASET_NAME}"
MEADOW_SUBNATIONAL_DATASET_PATH = DATA_DIR / "meadow/rff/2022-09-14/world_carbon_pricing__subnational"
# Details of the output tables.
GARDEN_MAIN_TABLE_NAME = MEADOW_DATASET_NAME
GARDEN_VERSION = MEADOW_VERSION
GARDEN_ANY_SECTOR_TABLE_NAME = "world_carbon_pricing_any_sector"
# Get naming convention.
N = Names(str(CURRENT_DIR / MEADOW_DATASET_NAME))

# Labels for the variables showing whether any sector is covered by an ETS or a carbon tax at the national or only
# sub-national level.
LABEL_ETS_NOT_COVERED = "No ETS"
LABEL_ETS_COVERED = "Has an ETS"
LABEL_ETS_COVERED_ONLY_SUBNATIONAL = "Has an ETS only at a sub-national level"
LABEL_TAX_NOT_COVERED = "No carbon tax"
LABEL_TAX_COVERED = "Has a carbon tax"
LABEL_TAX_COVERED_ONLY_SUBNATIONAL = "Has a carbon tax only at a sub-national level"

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

# Columns to use as index in main table.
INDEX_COLUMNS = ["country", "year", "ipcc_code", "product"]
# Columns to use as index in table simplified to show whether there is coverage for any sector.
INDEX_COLUMNS_ANY_SECTOR = ["country", "year"]

# Mapping of countries and the regions of the country included in the sub-national dataset.
# In the future, it would be good to load this mapping as additional data (however, the mapping is hardcoded in the
# original repository, so it's not trivial to get this mapping automatically).
COUNTRY_MEMBERS_FILE = STEP_DIR / f"data/garden/rff/{GARDEN_VERSION}/sub_national_jurisdictions.json"


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


def get_coverage_for_any_sector(df: pd.DataFrame) -> pd.DataFrame:
    """Create a dataframe showing whether a country has any sector covered by an ets/carbon tax.

    Parameters
    ----------
    df : pd.DataFrame
        Original national or sub-national data, disaggregated by sector.

    Returns
    -------
    df_any_sector : pd.DataFrame
        Coverage for any sector.

    """
    # Create a simplified dataframe that gives, for each country and year, whether the country has any sector(-fuel)
    # that is covered by at least one tax instrument. And idem for ets.
    df_any_sector = (
        df.reset_index()
        .groupby(["country", "year"], observed=True)
        .agg({"ets": lambda x: min(x.sum(), 1), "tax": lambda x: min(x.sum(), 1)})
        .astype(int)
        .reset_index()
    )

    return df_any_sector


def prepare_subnational_data(df_subnational: pd.DataFrame, country_members: Dict[str, List[str]]) -> pd.DataFrame:
    """Create a dataframe showing whether a country has any sub-national jurisdiction for which any sector is covered by
    an ets/carbon tax.

    The 'country' column of this dataframe does not need to be harmonized, since we are mapping the original
    sub-national jurisdiction names to the harmonized name of the country.

    Parameters
    ----------
    df_subnational : pd.DataFrame
        Sub-national data, disaggregated by sector.

    Returns
    -------
    pd.DataFrame
        Processed sub-national data.

    """
    # Prepare subnational data.
    df_subnational = prepare_data(df_subnational)
    # Map subnational regions to their corresponding country.
    subregions_to_country = {
        subregion: country for country in list(country_members) for subregion in country_members[country]
    }
    df_subnational["country"] = dataframes.map_series(
        series=df_subnational["country"],
        mapping=subregions_to_country,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
    )
    # Get coverage of "any sector", where we only care about having at least one sector covered by carbon tax/ets.
    df_subnational = get_coverage_for_any_sector(df=df_subnational)

    return df_subnational


def combine_national_and_subnational_data(
    df_any_sector_national: pd.DataFrame, df_any_sector_subnational: pd.DataFrame
) -> pd.DataFrame:
    """Combine national and sub-national data on whether countries have any sector covered by a tax instrument.

    The returned dataframe will have three labels:
    * Whether a country-year had no sector covered.
    * Whether a country-year had at least on sector covered at the national level.
    * Whether a country-year had at least one sector in one sub-national jurisdiction covered, but no sector covered at
      the national level.

    We disregard whether a country has coverage both at the sub-national or at the national level.

    Parameters
    ----------
    df_any_sector_national : pd.DataFrame
        National data on whether countries have any sector covered by a tax instrument.
    df_any_sector_subnational : pd.DataFrame
        Sub-national data on whether countries have any sector covered by a tax instrument.

    Returns
    -------
    df_any_sector : pd.DataFrame
        Combined dataframe showing whether a country has at least one sector covered by a tax instrument at a national
        level, or only at the sub-national level, or not at all.

    """
    # Now combine national and subnational data.
    # If there is both subnational and national coverage, keep the latter.
    # To do so, replace rows in national coverage dataframe by nans, then combine national an subnational data
    # so that, in overlapping rows, the nans from the national dataframe will be replaced by the subnational data,
    # but if there is coverage in the national dataframe, it will be kept, ignoring subnational coverage.
    # Create new temporary labels, so that:
    # nan -> not covered, 1 -> national coverage, 2 -> subnational coverage.
    _df_any_sector_national = df_any_sector_national.replace({0: np.nan})
    _df_any_sector_subnational = df_any_sector_subnational.replace({1: 2})
    df_any_sector = dataframes.combine_two_overlapping_dataframes(
        df1=_df_any_sector_national,
        df2=_df_any_sector_subnational,
        index_columns=["country", "year"],
        keep_column_order=True,
    )
    # Now replace again those nans back to zeros.
    df_any_sector = df_any_sector.fillna(0)

    # Now replace 0, 1, and 2 by their corresponding labels.
    ets_mapping = {0: LABEL_ETS_NOT_COVERED, 1: LABEL_ETS_COVERED, 2: LABEL_ETS_COVERED_ONLY_SUBNATIONAL}
    tax_mapping = {0: LABEL_TAX_NOT_COVERED, 1: LABEL_TAX_COVERED, 2: LABEL_TAX_COVERED_ONLY_SUBNATIONAL}
    df_any_sector["ets"] = dataframes.map_series(series=df_any_sector["ets"], mapping=ets_mapping)
    df_any_sector["tax"] = dataframes.map_series(series=df_any_sector["tax"], mapping=tax_mapping)

    return cast(pd.DataFrame, df_any_sector)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Read main dataset from meadow.
    ds_meadow = Dataset(MEADOW_MAIN_DATASET_PATH)

    # Read subnational dataset from meadow.
    ds_meadow_subnational = Dataset(MEADOW_SUBNATIONAL_DATASET_PATH)

    # Get main table from dataset.
    tb_meadow = ds_meadow[ds_meadow.table_names[0]]

    # Get table for subnational data from dataset.
    tb_meadow_subnational = ds_meadow_subnational[ds_meadow_subnational.table_names[0]]

    # Construct a dataframe from the main table.
    df = pd.DataFrame(tb_meadow)

    # Construct a dataframe for subnational data.
    df_subnational = pd.DataFrame(tb_meadow_subnational)

    # Load dictionary mapping sub-national jurisdictions to their countries.
    country_members = io.local.load_json(COUNTRY_MEMBERS_FILE)

    #
    # Process data.
    #
    # Sanity checks on raw data.
    sanity_checks(df=df)
    sanity_checks(df=df_subnational)

    # Prepare data.
    df = prepare_data(df=df)

    # Harmonize country names.
    df = geo.harmonize_countries(df=df, countries_file=str(N.country_mapping_path), warn_on_unused_countries=False)

    # Create a simplified table for "any sector" of national data.
    df_any_sector_national = get_coverage_for_any_sector(df=df)

    # Create a simplified dataframe with the coverage for "any sector" of subnational data.
    df_any_sector_subnational = prepare_subnational_data(df_subnational=df_subnational, country_members=country_members)

    # Combine national and subnational data.
    df_any_sector = combine_national_and_subnational_data(
        df_any_sector_national=df_any_sector_national, df_any_sector_subnational=df_any_sector_subnational
    )

    # Prepare output tables.
    tb = underscore_table(Table(df)).set_index(INDEX_COLUMNS, verify_integrity=True).sort_index().sort_index(axis=1)
    tb_any_sector = (
        underscore_table(Table(df_any_sector))
        .set_index(INDEX_COLUMNS_ANY_SECTOR, verify_integrity=True)
        .sort_index()
        .sort_index(axis=1)
    )

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
    tb.update_metadata_from_yaml(N.metadata_path, GARDEN_MAIN_TABLE_NAME)

    # Update simplified table metadata using metadata yaml file.
    tb_any_sector.update_metadata_from_yaml(N.metadata_path, GARDEN_ANY_SECTOR_TABLE_NAME)

    # Add tables to dataset.
    ds_garden.add(tb)
    ds_garden.add(tb_any_sector)

    # Save dataset.
    ds_garden.save()

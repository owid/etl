from typing import Dict, List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.datautils import dataframes, io
from shared import CURRENT_DIR

from etl.data_helpers import geo
from etl.helpers import PathFinder
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
N = PathFinder(str(CURRENT_DIR / MEADOW_DATASET_NAME))

# Labels for the variables showing whether any sector is covered by an ETS or a carbon tax at the national or only
# sub-national level.
LABEL_ETS_NOT_COVERED = "No ETS"
LABEL_ETS_COVERED = "Has an ETS"
LABEL_ETS_COVERED_ONLY_SUBNATIONAL = "Has an ETS only at a sub-national level"
LABEL_TAX_NOT_COVERED = "No carbon tax"
LABEL_TAX_COVERED = "Has a carbon tax"
LABEL_TAX_COVERED_ONLY_SUBNATIONAL = "Has a carbon tax only at a sub-national level"
# If a country-years has both national and subnational coverage, mention only the national and ignore subnational.
LABEL_ETS_COVERED_NATIONAL_AND_SUBNATIONAL = "Has an ETS"
LABEL_TAX_COVERED_NATIONAL_AND_SUBNATIONAL = "Has a carbon tax"

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

    The returned dataframe will have the following labels:
    * Whether a country-year has no sector covered.
    * Whether a country-year has at least one sector covered at the national level.
    * Whether a country-year has at least one sector in one sub-national jurisdiction covered, but no sector covered at
      the national level.
    * Whether a country-year has at least one sector in both a sub-national and the national jurisdiction covered.
      However, for now we disregard this option, by using the same label as for only national coverage.

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
    # Combine national and subnational data.
    df_any_sector = pd.merge(
        df_any_sector_national,
        df_any_sector_subnational,
        on=["country", "year"],
        how="left",
        suffixes=("_national", "_subnational"),
    ).fillna(0)

    # Create two new columns ets and tax, that are:
    # * 0 if no ets/tax exists.
    # * 1 if there is a national ets/tax and not a subnational ets/tax.
    # * 2 if there is a subnational ets/tax and not a national ets/tax.
    # * 3 if there are both a national and a subnational ets/tax.
    df_any_sector = df_any_sector.assign(
        **{
            "ets": df_any_sector["ets_national"] + 2 * df_any_sector["ets_subnational"],
            "tax": df_any_sector["tax_national"] + 2 * df_any_sector["tax_subnational"],
        }
    )[["country", "year", "ets", "tax"]]

    # Now replace 0, 1, 2, and 3 by their corresponding labels.
    ets_mapping = {
        0: LABEL_ETS_NOT_COVERED,
        1: LABEL_ETS_COVERED,
        2: LABEL_ETS_COVERED_ONLY_SUBNATIONAL,
        3: LABEL_ETS_COVERED_NATIONAL_AND_SUBNATIONAL,
    }
    tax_mapping = {
        0: LABEL_TAX_NOT_COVERED,
        1: LABEL_TAX_COVERED,
        2: LABEL_TAX_COVERED_ONLY_SUBNATIONAL,
        3: LABEL_TAX_COVERED_NATIONAL_AND_SUBNATIONAL,
    }
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
    country_members = io.load_json(COUNTRY_MEMBERS_FILE)

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
    tb = (
        Table(df, short_name=GARDEN_MAIN_TABLE_NAME, underscore=True)
        .set_index(INDEX_COLUMNS, verify_integrity=True)
        .sort_index()
        .sort_index(axis=1)
    )
    tb_any_sector = (
        Table(df_any_sector, short_name=GARDEN_ANY_SECTOR_TABLE_NAME, underscore=True)
        .set_index(INDEX_COLUMNS_ANY_SECTOR, verify_integrity=True)
        .sort_index()
        .sort_index(axis=1)
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    ds_garden.add(tb)
    ds_garden.add(tb_any_sector)
    ds_garden.update_metadata(N.metadata_path)

    # Save dataset.
    ds_garden.save()

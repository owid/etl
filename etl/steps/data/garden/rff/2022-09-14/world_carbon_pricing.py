# +
from typing import List

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.datautils import dataframes, geo
from shared import CURRENT_DIR

from etl.helpers import Names
from etl.paths import DATA_DIR
# -

MEADOW_DATASET_NAME = "world_carbon_pricing"
MEADOW_MAIN_DATASET_PATH = DATA_DIR / f"meadow/rff/2022-09-14/{MEADOW_DATASET_NAME}"
MEADOW_SUBNATIONAL_DATASET_PATH = DATA_DIR / "meadow/rff/2022-09-14/world_carbon_pricing__subnational"
GARDEN_MAIN_TABLE_NAME = MEADOW_DATASET_NAME
GARDEN_ANY_SECTOR_TABLE_NAME = "world_carbon_pricing_any_sector"
# Get naming convention.
N = Names(str(CURRENT_DIR / MEADOW_DATASET_NAME))

LABEL_ETS_NOT_COVERED = "No emissions trading system"
LABEL_ETS_COVERED = "Has an emissions trading system"
LABEL_ETS_COVERED_ONLY_SUBNATIONAL = "Has an emissions trading system only at subnational level"
LABEL_TAX_NOT_COVERED = "No carbon tax"
LABEL_TAX_COVERED = "Has a carbon tax"
LABEL_TAX_COVERED_ONLY_SUBNATIONAL = "Has a carbon tax only at subnational level"

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

country_members = {"Canada": ['Alberta',
             'British Columbia',
             'Manitoba',
             'New Brunswick',
             'Newfoundland and Labrador',
             'Northwest Territories',
             'Nova Scotia',
             'Nunavut',
             'Ontario',
             'Prince Edward Island',
             'Quebec',
             'Saskatchewan',
             'Yukon',],
                   "China": ['Anhui Province',
             'Beijing Municipality',
             'Chongqing Municipality',
             'Fujian Province',
             'Gansu Province',
             'Guangdong Province',
             'Guangxi Zhuang Autonomous Region',
             'Guizhou Province',
             'Hainan Province',
             'Hebei Province',
             'Heilongjiang Province',
             'Henan Province',
             'Hong Kong Special Administrative Region',
             'Hubei Province',
             'Hunan Province',
             'Inner Mongolia Autonomous Region',
             'Jiangsu Province',
             'Jiangxi Province',
             'Jilin Province',
             'Liaoning Province',
             'Macau Special Administrative Region',
             'Ningxia Hui Autonomous Region',
             'Qinghai Province',
             'Shaanxi Province',
             'Shandong Province',
             'Shanghai Municipality',
             'Shanxi Province',
             'Shenzhen',
             'Sichuan Province',
             'Tianjin Municipality',
             'Tibet Autonomous Region',
             'Xinjiang Uyghur Autonomous Region',
             'Yunnan Province',
             'Zhejiang Province',],
                   "Japan": ["Aichi",
              "Akita",
              "Aomori",
              "Chiba",
              "Ehime",
              "Fukui",
              "Fukuoka",
              "Fukushima",
              "Gifu",
              "Gunma",
              "Hiroshima",
              "Hokkaido",
              "Hyogo",
              "Ibaraki",
              "Ishikawa",
              "Iwate",
              "Kagawa",
              "Kagoshima",
              "Kanagawa",
              "Kochi",
              "Kumamoto",
              "Kyoto",
              "Mie",
              "Miyagi",
              "Miyazaki",
              "Nagano",
              "Nagasaki",
              "Nara",
              "Niigata",
              "Oita",
              "Okayama",
              "Okinawa",
              "Osaka",
              "Saga",
              "Saitama",
              "Shiga",
              "Shimane",
              "Shizuoka",
              "Tochigi",
              "Tokushima",
              "Tokyo",
              "Tottori",
              "Toyama",
              "Wakayama",
              "Yamagata",
              "Yamaguchi",
              "Yamanashi",],
                   "United States": ['Alabama',
             'Alaska',
             'Arizona',
             'Arkansas',
             'California',
             'Colorado',
             'Connecticut',
             'Delaware',
             'Florida',
             'Georgia_US',
             'Hawaii',
             'Idaho',
             'Illinois',
             'Indiana',
             'Iowa',
             'Kansas',
             'Kentucky',
             'Louisiana',
             'Maine',
             'Maryland',
             'Massachusetts',
             'Michigan',
             'Minnesota',
             'Mississippi',
             'Missouri',
             'Montana',
             'Nebraska',
             'Nevada',
             'New Hampshire',
             'New Jersey',
             'New Mexico',
             'New York',
             'North Carolina',
             'North Dakota',
             'Ohio',
             'Oklahoma',
             'Oregon',
             'Pennsylvania',
             'Rhode Island',
             'South Carolina',
             'South Dakota',
             'Tennessee',
             'Texas',
             'Utah',
             'Vermont',
             'Virginia',
             'Washington',
             'West Virginia',
             'Wisconsin',
             'Wyoming',],
                   "Mexico": ["Aguascalientes",
                "Baja California",
                "Baja California Sur",
                "Campeche",
                "Chiapas",
                "Chihuahua",
                "Coahuila de Zaragoza",
                "Colima",
                "Durango",
                "Guanajuato",
                "Guerrero",
                "Hidalgo",
                "Jalisco",
                "Mexico State",
                "Ciudad de Mexico",
                "Michoacan de Ocampo",
                "Morelos",
                "Nayarit",
                "Nuevo Leon",
                "Oaxaca",
                "Puebla",
                "Queretaro de Arteaga",
                "Quintana Roo",
                "San Luis Potosi",
                "Sinaloa",
                "Sonora",
                "Tabasco",
                "Tamaulipas",
                "Tlaxcala",
                "Veracruz de Ignacio de la Llave",
                "Yucatan",],
                  }


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


def get_coverage_for_any_sector(df: pd.DataFrame, subnational: bool) -> pd.DataFrame:
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


def prepare_subnational_data(df_subnational: pd.DataFrame) -> pd.DataFrame:
    # Prepare subnational data.
    df_subnational = prepare_data(df_subnational)
    # Map subnational regions to their corresponding country.
    subregions_to_country = {subregion: country for country in list(country_members) for subregion in country_members[country]}
    df_subnational["country"] = dataframes.map_series(series=df_subnational["country"], mapping=subregions_to_country, warn_on_missing_mappings=True, warn_on_unused_mappings=True)
    # Get coverage of "any sector", where we only care about having at least one sector covered by carbon tax/ets.
    df_subnational = get_coverage_for_any_sector(df=df_subnational, subnational=True)

    return df_subnational


def combine_national_and_subnational_data(df_any_sector_national: pd.DataFrame, df_any_sector_subnational: pd.DataFrame) -> pd.DataFrame:
    # Now combine national and subnational data.
    # If there is both subnational and national coverage, keep the latter.
    # To do so, replace rows in national coverage dataframe by nans, then combine national an subnational data
    # so that, in overlapping rows, the nans from the national dataframe will be replaced by the subnational data,
    # but if there is coverage in the national dataframe, it will be kept, ignoring subnational coverage.
    # Create new temporary labels, so that:
    # nan -> not covered, 1 -> national coverage, 2 -> subnational coverage.
    _df_any_sector_national = df_any_sector_national.replace({0: np.nan})
    _df_any_sector_subnational = df_any_sector_subnational.replace({1: 2})
    df_any_sector = dataframes.combine_two_overlapping_dataframes(df1=_df_any_sector_national, df2=_df_any_sector_subnational, index_columns=["country", "year"], keep_column_order=True)
    # Now replace again those nans back to zeros.
    df_any_sector = df_any_sector.fillna(0)

    # Now replace 0, 1, and 2 by their corresponding labels.
    ets_mapping = {0: LABEL_ETS_NOT_COVERED, 1: LABEL_ETS_COVERED, 2: LABEL_ETS_COVERED_ONLY_SUBNATIONAL}
    tax_mapping = {0: LABEL_TAX_NOT_COVERED, 1: LABEL_TAX_COVERED, 2: LABEL_TAX_COVERED_ONLY_SUBNATIONAL}
    df_any_sector["ets"] = dataframes.map_series(series=df_any_sector["ets"], mapping=ets_mapping)
    df_any_sector["tax"] = dataframes.map_series(series=df_any_sector["tax"], mapping=tax_mapping)

    return df_any_sector


def create_output_table(df: pd.DataFrame, index_columns: List[str]) -> Table:
    tb = Table(df)
    tb = tb.set_index(index_columns, verify_integrity=True).sort_index().sort_index(axis=1)
    tb = underscore_table(tb)

    return tb


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

    #
    # Process data.
    #
    # Sanity checks on raw data.
    sanity_checks(df=df)

    # Prepare data.
    df = prepare_data(df=df)

    # Harmonize country names.
    df = geo.harmonize_countries(df=df, countries_file=str(N.country_mapping_path), warn_on_unused_countries=False)

    # Create a simplified table for "any sector" of national data.
    df_any_sector_national = get_coverage_for_any_sector(df=df, subnational=False)

    # Create a simplified dataframe with the coverage for "any sector" of subnational data.
    df_any_sector_subnational = prepare_subnational_data(df_subnational=df_subnational)

    # Combine national and subnational data.
    df_any_sector = combine_national_and_subnational_data(df_any_sector_national=df_any_sector_national, df_any_sector_subnational=df_any_sector_subnational)

    # Prepare output tables.
    tb = create_output_table(df=df, index_columns=INDEX_COLUMNS)
    tb_any_sector = create_output_table(df=df_any_sector, index_columns=INDEX_COLUMNS_ANY_SECTOR)

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

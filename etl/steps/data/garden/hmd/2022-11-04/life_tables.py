import json
from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.datautils import geo
from structlog import get_logger

from etl.helpers import Names
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
N = Names(__file__)

SHORT_NAME = "life_tables"
MEADOW_VERSION = "2022-11-04"
MEADOW_DATASET = DATA_DIR / f"meadow/hmd/{MEADOW_VERSION}/{SHORT_NAME}"


def run(dest_dir: str) -> None:
    log.info("life_tables.start")

    # read dataset from meadow
    ds_meadow = Dataset(MEADOW_DATASET)

    # init dataset
    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata
    print(N.metadata_path)
    ds_garden.metadata.update_from_yaml(N.metadata_path)

    # build tables
    tables_names = ds_meadow.table_names
    for table_name in tables_names:
        log.info(f"life_tables:{table_name}.start")
        tb_garden = make_table(ds_meadow, table_name)
        ds_garden.add(tb_garden)
        ds_garden.save()
        log.info(f"life_tables:{table_name}.end")

    log.info("life_tables.end")


def make_table(ds_meadow: Dataset, table_name: str) -> Table:
    log.info(f"Building table {table_name}...")
    tb_meadow = ds_meadow[table_name]

    # Country management
    df = pd.DataFrame(tb_meadow)
    df = clean_countries(df)

    # Build table
    tb_garden = underscore_table(Table(df))
    tb_garden.metadata = tb_meadow.metadata

    # Edit variables
    for col in tb_garden.columns:
        tb_garden[col].metadata = tb_meadow[col].metadata

    # Edit table
    tb_garden.update_metadata_from_yaml(N.metadata_path, table_name)
    tb_garden = tb_garden.set_index(["country", "year", "age"], verify_integrity=True)

    return tb_garden


def clean_countries(df: pd.DataFrame) -> pd.DataFrame:
    df = exclude_countries(df)
    df = harmonize_countries(df)
    return df


def load_excluded_countries() -> List[str]:
    with open(N.excluded_countries_path, "r") as f:
        data = json.load(f)
        assert isinstance(data, list)
    return data


def exclude_countries(df: pd.DataFrame) -> pd.DataFrame:
    excluded_countries = load_excluded_countries()
    return cast(pd.DataFrame, df.loc[~df.country.isin(excluded_countries)])


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    unharmonized_countries = df["country"]
    df = geo.harmonize_countries(df=df, countries_file=str(N.country_mapping_path))

    missing_countries = set(unharmonized_countries[df.country.isnull()])
    if any(missing_countries):
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {N.country_mapping_path} to include these country "
            f"names; or (b) add them to {N.excluded_countries_path}."
            f"Raw country names: {missing_countries}"
        )

    return df

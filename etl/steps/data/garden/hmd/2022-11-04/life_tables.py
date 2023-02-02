import json
from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
N = PathFinder(__file__)

SHORT_NAME = "life_tables"
MEADOW_VERSION = "2022-11-04"
MEADOW_DATASET = DATA_DIR / f"meadow/hmd/{MEADOW_VERSION}/{SHORT_NAME}"


def run(dest_dir: str) -> None:
    log.info("life_tables.start")

    # read dataset from meadow
    ds_meadow = Dataset(MEADOW_DATASET)

    # init dataset
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # build tables
    tables_names = ds_meadow.table_names
    for table_name in tables_names:
        log.info(f"life_tables:{table_name}.start")
        tb_garden = make_table(ds_meadow, table_name)
        ds_garden.add(tb_garden)
        log.info(f"life_tables:{table_name}.end")

    ds_garden.update_metadata(N.metadata_path)
    ds_garden.save()

    log.info("life_tables.end")


def make_table(ds_meadow: Dataset, table_name: str) -> Table:
    log.info(f"Building table {table_name}...")

    # Country management
    tb_garden = ds_meadow[table_name].reset_index(drop=True)
    tb_garden = clean_countries(tb_garden)
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

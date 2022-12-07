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
VERSION_MEADOW = "2022-12-07"
DATASET_MEADOW = DATA_DIR / f"meadow/hmd/{VERSION_MEADOW}/{SHORT_NAME}"

TABLE_MAPPING = {
    "period_1x1": ["both_1x1", "female_1x1", "male_1x1"],
    "period_1x5": ["both_1x5", "female_1x5", "male_1x5"],
    "period_1x10": ["both_1x10", "female_1x10", "male_1x10"],
    "period_5x1": ["both_5x1", "female_5x1", "male_5x1"],
    "period_5x5": ["both_5x5", "female_5x5", "male_5x5"],
    "period_5x10": ["both_5x10", "female_5x10", "male_5x10"],
}


def run(dest_dir: str) -> None:
    log.info("life_tables.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATASET_MEADOW)

    # init dataset
    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata
    ds_garden.metadata.update_from_yaml(N.metadata_path)

    # build tables
    for table_new, tables_old in TABLE_MAPPING.items():
        log.info(f"life_tables: {table_new}.start")
        tb_garden = make_table(ds_meadow, tables_old, table_new)
        ds_garden.add(tb_garden)
        ds_garden.save()
        log.info(f"life_tables: {table_new}.end")

    log.info("life_tables.end")


def make_table(ds_meadow: Dataset, tables_old_names: List[str], table_new_name: str) -> Table:
    log.info(f"life_tables: building table {table_new_name} from {tables_old_names}...")
    # Combine multiple tables (broken down by sex) into single one (insert sex in index)
    df = combine_sex_tables(ds_meadow, tables_old_names)

    # Country management
    df = clean_countries(df)

    # Build table
    tb_garden = underscore_table(Table(df))

    # Edit table
    tb_garden.update_metadata_from_yaml(N.metadata_path, table_new_name)
    tb_garden = tb_garden.set_index(["country", "year", "age", "sex"], verify_integrity=True).sort_index()

    return tb_garden


def combine_sex_tables(ds_meadow: Dataset, table_names: List[str]) -> pd.DataFrame:
    dfs = []
    for table_name in table_names:
        tb_meadow = ds_meadow[table_name]
        # Country management
        df = pd.DataFrame(tb_meadow)
        # Sex
        if table_name.startswith("both_"):
            df = df.assign(sex="all")
        elif table_name.startswith("female_"):
            df = df.assign(sex="female")
        elif table_name.startswith("male_"):
            df = df.assign(sex="male")
        else:
            raise ValueError(f"Unknown sex for table {table_name}!")
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)
    return df


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

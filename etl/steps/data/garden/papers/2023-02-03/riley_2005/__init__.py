import json
from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
N = PathFinder(__file__)
COUNTRY_MAPPING_PATH = N.directory / "countries.json"
EXCLUDED_COUNTRIES_PATH = N.directory / "excluded_countries.json"
METADATA_PATH = N.directory / "meta.yml"


def run(dest_dir: str) -> None:
    log.info("riley_2005.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/papers/2023-02-03/riley_2005")
    tb_meadow = ds_meadow["riley_2005"]

    df = pd.DataFrame(tb_meadow)

    log.info("riley_2005.exclude_countries")
    df = exclude_countries(df)

    log.info("riley_2005.harmonize_countries")
    df = harmonize_countries(df)

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    tb_garden = underscore_table(Table(df))
    tb_garden.metadata = tb_meadow.metadata
    for col in tb_garden.columns:
        tb_garden[col].metadata = tb_meadow[col].metadata

    ds_garden.metadata.update_from_yaml(METADATA_PATH)
    tb_garden.update_metadata_from_yaml(METADATA_PATH, "riley_2005")

    tb_garden = tb_garden.set_index(["entity", "year"])

    ds_garden.add(tb_garden)
    ds_garden.save()

    log.info("riley_2005.end")


def load_excluded_countries() -> List[str]:
    with open(EXCLUDED_COUNTRIES_PATH, "r") as f:
        data = json.load(f)
        assert isinstance(data, list)
    return data


def exclude_countries(df: pd.DataFrame) -> pd.DataFrame:
    excluded_countries = load_excluded_countries()
    return cast(pd.DataFrame, df.loc[~df.entity.isin(excluded_countries)])


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    unharmonized_countries = df["entity"]
    df = geo.harmonize_countries(df=df, countries_file=str(COUNTRY_MAPPING_PATH), country_col="entity")

    missing_countries = set(unharmonized_countries[df.entity.isnull()])
    if any(missing_countries):
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {COUNTRY_MAPPING_PATH} to include these country "
            f"names; or (b) add them to {EXCLUDED_COUNTRIES_PATH}."
            f"Raw country names: {missing_countries}"
        )

    return df

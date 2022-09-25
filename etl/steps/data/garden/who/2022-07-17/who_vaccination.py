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
N = Names("/Users/fionaspooner/Documents/OWID/repos/etl/etl/steps/data/garden/who/2022-07-17/who_vaccination.py")


def run(dest_dir: str) -> None:
    log.info("who_vaccination.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/who/2022-07-17/who_vaccination")
    tb_meadow = ds_meadow["who_vaccination"]

    df = pd.DataFrame(tb_meadow)

    log.info("who_vaccination.exclude_countries")
    df = exclude_countries(df)

    log.info("who_vaccination.harmonize_countries")
    df = harmonize_countries(df)

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    # may need to combine japanese encephalitis and japanese encephalitis first dose
    # We use only the official figures
    df = df[df["coverage_category"] == "OFFICIAL"]
    df = df.dropna(subset="coverage")
    df = df.drop(columns=["index", "group", "antigen", "coverage_category", "coverage_category_description"])
    df = df.pivot_table(
        values=["coverage", "doses", "target_number"],
        columns=["antigen_description"],
        index=[
            "country",
            "year",
        ],
    )
    df.columns = df.columns.to_series().apply("_".join).str.replace("coverage_", "")
    df = df.reset_index()

    tb_garden = underscore_table(Table(df))

    tb_garden.metadata = tb_meadow.metadata
    ds_garden.metadata.update_from_yaml(N.metadata_path)
    tb_garden.update_metadata_from_yaml(N.metadata_path, "who_vaccination")

    ds_garden.add(tb_garden)
    ds_garden.save()

    log.info("who_vaccination.end")


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

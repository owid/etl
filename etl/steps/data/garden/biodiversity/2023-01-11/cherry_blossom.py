import json
from typing import List, cast

import pandas as pd
from owid import catalog
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import Names
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
N = Names(__file__)


def run(dest_dir: str) -> None:
    log.info("cherry_blossom.start")

    # read dataset from meadow
    ds_meadow = catalog.Dataset(DATA_DIR / "meadow/biodiversity/2023-01-11/cherry_blossom")
    tb_meadow = ds_meadow["cherry_blossom"]

    df = pd.DataFrame(tb_meadow)

    log.info("cherry_blossom.exclude_countries")
    df = exclude_countries(df)

    log.info("cherry_blossom.harmonize_countries")
    df = harmonize_countries(df)

    # Calculate a 20 year average
    df = calculate_multiple_year_average(df)

    # create new dataset with the same metadata as meadow
    ds_garden = catalog.Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # create new table with the same metadata as meadow and add it to dataset
    tb_garden = catalog.Table(df, metadata=N.metadata_path)
    ds_garden.add(tb_garden)

    # update metadata from yaml file
    ds_garden.update_metadata(N.metadata_path)

    ds_garden.save()

    log.info("cherry_blossom.end")


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


def calculate_multiple_year_average(df: pd.DataFrame) -> pd.DataFrame:
    min_year = df["year"].min()
    max_year = df["year"].max()

    df_year = pd.DataFrame()
    df_year["year"] = pd.Series(range(min_year, max_year))
    df_year["country"] = "Japan"
    df_comb = pd.merge(df, df_year, how="outer", on=["country", "year"])

    df_comb = df_comb.sort_values("year")

    df_comb["average_20_years"] = df_comb["full_flowering_date"].rolling(20, min_periods=1).mean()

    return df_comb

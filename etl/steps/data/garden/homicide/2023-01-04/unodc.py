import json
from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore, underscore_table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("unodc.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/homicide/2023-01-04/unodc")
    tb_meadow = ds_meadow["unodc"]

    df = pd.DataFrame(tb_meadow)

    log.info("unodc.exclude_countries")
    df = exclude_countries(df)

    log.info("unodc.harmonize_countries")
    df = harmonize_countries(df)

    df = clean_data(df)
    # create new dataset with the same metadata as meadow
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # create new table with the same metadata as meadow and add it to dataset
    tb_garden = underscore_table(Table(df, short_name=tb_meadow.metadata.short_name))
    ds_garden.add(tb_garden)

    # update metadata from yaml file
    ds_garden.update_metadata(paths.metadata_path)

    ds_garden.save()

    log.info("unodc.end")


def load_excluded_countries() -> List[str]:
    with open(paths.excluded_countries_path, "r") as f:
        data = json.load(f)
        assert isinstance(data, list)
    return data


def exclude_countries(df: pd.DataFrame) -> pd.DataFrame:
    excluded_countries = load_excluded_countries()
    return cast(pd.DataFrame, df.loc[~df.country.isin(excluded_countries)])


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    unharmonized_countries = df["country"]
    df = geo.harmonize_countries(df=df, countries_file=str(paths.country_mapping_path))

    missing_countries = set(unharmonized_countries[df.country.isnull()])
    if any(missing_countries):
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {paths.country_mapping_path} to include these country "
            f"names; or (b) add them to {paths.excluded_countries_path}."
            f"Raw country names: {missing_countries}"
        )

    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:

    df = df[["country", "year", "unit_of_measurement", "value"]]

    df = (
        df.pivot(index=["country", "year"], columns="unit_of_measurement", values="value")
        .reset_index()
        .rename_axis(None, axis=1)
    )

    return df

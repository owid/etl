from copy import deepcopy
from pathlib import Path

import pandas as pd
from owid.catalog import Dataset, Table

from etl.paths import DATA_DIR
from etl.steps.data.garden.owid.latest.key_indicators.utils import add_regions

DIR_PATH = Path(__file__).parent


def load_land_area(ds: Dataset) -> Table:
    d = Dataset(DATA_DIR / "open_numbers/open_numbers/latest/open_numbers__world_development_indicators")
    table = d["ag_lnd_totl_k2"]

    table = table.reset_index()

    # convert iso codes to country names
    countries_regions = Dataset(DATA_DIR / "garden/regions/2023-01-01/regions")["regions"]

    table = (
        table.rename(
            columns={
                "time": "year",
                "ag_lnd_totl_k2": "land_area",
            }
        )
        .assign(country=table.geo.str.upper().map(countries_regions["name"]))
        .dropna(subset=["country"])
        .drop(["geo"], axis=1)
        .pipe(add_regions, population=ds["population"].reset_index())
        .pipe(add_world)
    )

    return table.set_index(["country", "year"])


def add_world(df: pd.DataFrame) -> pd.DataFrame:
    """Add world aggregates.

    We do this by adding the values for all continents.
    """
    assert "World" not in set(df.country), "World already in data!"
    df_ = deepcopy(df)
    continents = [
        "Europe",
        "Asia",
        "North America",
        "South America",
        "Africa",
        "Oceania",
    ]
    df_ = (
        df_[(df_["country"].isin(continents))]
        .groupby("year", as_index=False)["land_area"]
        .sum()
        .assign(country="World")
    )
    df = pd.concat([df, df_], ignore_index=True).sort_values(["country", "year"])
    return df


def make_table(ds: Dataset) -> Table:
    t = load_land_area(ds)
    t.update_metadata_from_yaml(DIR_PATH / "key_indicators.meta.yml", "land_area")

    # variable ID 147839 in grapher
    t.land_area.display = {"unit": "%"}

    return t

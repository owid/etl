from pathlib import Path

from owid import catalog
from owid.catalog import Dataset, Table
import pandas as pd

from copy import deepcopy

from etl.paths import DATA_DIR, REFERENCE_DATASET
from etl.steps.data.garden.owid.latest.key_indicators.utils import add_regions


DIR_PATH = Path(__file__).parent


def load_land_area() -> Table:
    d = Dataset(DATA_DIR / "open_numbers/open_numbers/latest/open_numbers__world_development_indicators")
    table = d["ag_lnd_totl_k2"]

    table = table.reset_index()

    # convert iso codes to country names
    reference_dataset = catalog.Dataset(REFERENCE_DATASET)
    countries_regions = reference_dataset["countries_regions"]

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
        .pipe(add_regions)
        .pipe(add_world)
    )

    return table.set_index(["country", "year"])


def add_world(df: pd.DataFrame) -> pd.DataFrame:
    """Add world aggregates."""
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
    df_ = df_[(df_.country.isin(continents))].groupby("year", as_index=False).population.sum().assign(country="World")
    df = pd.concat([df, df_], ignore_index=True).sort_values(["country", "year"])
    return df


def make_table() -> Table:
    t = load_land_area()
    t.update_metadata_from_yaml(DIR_PATH / "key_indicators.meta.yml", "land_area")

    # variable ID 147839 in grapher
    t.land_area.display = {"unit": "%"}

    return t

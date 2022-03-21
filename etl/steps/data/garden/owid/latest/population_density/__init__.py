#
#  __init__.py
#  owid/latest/population_density
#

"""
Adapted from Ed's population density script:

https://github.com/owid/notebooks/blob/main/EdouardMathieu/omm_population_density/script.py
"""

import pandas as pd
from typing import cast
from pathlib import Path

from owid.catalog import Dataset, Table, Source

from etl.paths import DATA_DIR


KEY_INDICATORS = DATA_DIR / "garden/owid/latest/key_indicators"


def load_population() -> Table:
    return Dataset(KEY_INDICATORS)["population"].reset_index()


def load_land_area() -> Table:
    return Dataset(KEY_INDICATORS)["land_area"].reset_index()


def make_table() -> Table:
    population = load_population()
    land_area = load_land_area()

    # take the latest measurement of land area
    land_area = (
        land_area.sort_values("year").groupby(["country"], as_index=False).last()
    )

    df = pd.merge(
        land_area[["country", "land_area"]],
        population[["country", "population", "year"]],
        on="country",
        validate="one_to_many",
    )

    df = (
        df.assign(
            population_density=(df.population / df.land_area)
            .round(3)
            .rename("population_density")
        )
        .drop(columns=["population", "land_area"])
        .sort_values(["country", "year"])
    )

    assert (df.population_density >= 0).all()
    assert (df.population_density < 40000).all()
    return df.set_index(["country", "year"])


def load_sources() -> list[Source]:
    return cast(
        list[Source],
        load_population().population.metadata.sources
        + load_land_area().land_area.metadata.sources,
    )


def run(dest_dir: str) -> None:
    ds = Dataset.create_empty(dest_dir)
    meta_path = Path(__file__).parent / "population_density.meta.yml"
    ds.metadata.update_from_yaml(meta_path)
    ds.metadata.sources = load_sources()

    t = make_table()
    t.update_metadata_from_yaml(meta_path, "population_density")

    ds.add(t)
    ds.save()

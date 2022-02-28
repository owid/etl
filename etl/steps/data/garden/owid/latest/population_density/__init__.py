#
#  __init__.py
#  owid/latest/population_density
#

"""
Adapted from Ed's population density script:

https://github.com/owid/notebooks/blob/main/EdouardMathieu/omm_population_density/script.py
"""

import pandas as pd

from owid.catalog import Dataset, DatasetMeta, Table

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

    df.metadata.short_name = "population-density"
    df.metadata.description = "Population density (World Bank, Gapminder, HYDE & UN)"
    assert (df.population_density >= 0).all()
    assert (df.population_density < 40000).all()
    return df.set_index(["country", "year"])


def run(dest_dir: str) -> None:
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = DatasetMeta(
        namespace="owid",
        short_name="population_density",
        description="Population density (World Bank, Gapminder, HYDE & UN)",
    )

    t = make_table()
    ds.add(t)
    ds.save()

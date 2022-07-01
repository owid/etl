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

from owid.catalog import Dataset, DatasetMeta, Table, Variable, Source, VariableMeta

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
    t = make_table()

    # add metadata, use values from variable 123 in grapher
    ds.metadata = DatasetMeta(
        namespace="owid",
        short_name="population_density",
        title="Population density (World Bank, Gapminder, HYDE & UN)",
        sources=load_sources(),
    )
    t.metadata.short_name = "population_density"
    t.metadata.description = """
    Our World in Data builds and maintains a long-run dataset on population by country, region, and for the world, based on three key sources: HYDE, Gapminder, and the UN World Population Prospects. This combines historical population estimates with median scenario projections to 2100. You can find more information on these sources and how our time series is constructed on this page: <a href="https://ourworldindata.org/population-sources">What sources do we rely on for population estimates?</a>\n\nWe combine this population dataset with the <a href="https://ourworldindata.org/grapher/land-area-km">land area estimates published by the World Bank</a>, to produce a long-run dataset of population density.\n\nIn all sources that we rely on, population estimates and land area estimates are based on today’s geographical borders.'
    """.strip()

    # variable metadata (id 123 in grapher)
    t.population_density.metadata = VariableMeta(
        title="population_density",
        display={
            "name": "Population density",
            "unit": "people per km²",
            "includeInTable": True,
        },
    )

    ds.add(t)
    ds.save()


def set_variable_metadata(v: Variable, meta: VariableMeta) -> None:
    """Set Metadata on a variable.
    TODO: make this a method of the Variable class
    """
    v._fields[v.checked_name] = meta

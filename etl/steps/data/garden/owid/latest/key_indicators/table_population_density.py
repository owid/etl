#
#  __init__.py
#  owid/latest/population_density
#

"""
Adapted from Ed's population density script:

https://github.com/owid/notebooks/blob/main/EdouardMathieu/omm_population_density/script.py
"""

from pathlib import Path
from typing import cast

import pandas as pd
from owid.catalog import Dataset, Source, Table

DIR_PATH = Path(__file__).parent


def _combine_tables(population: Table, land_area: Table):
    return pd.merge(
        land_area[["country", "land_area"]],
        population[["country", "population", "year"]],
        on="country",
        validate="one_to_many",
    )


def _build_metric(t: Table) -> Table:
    return (
        t.assign(population_density=(t.population / t.land_area).round(3).rename("population_density"))
        .drop(columns=["population", "land_area"])
        .sort_values(["country", "year"])
    )


def _sanity_checks(t: Table) -> Table:
    assert (t.population_density >= 0).all()
    assert (t.population_density < 40000).all()


def _build_sources(population: Table, land_area: Table) -> list[Source]:
    return cast(
        list[Source],
        population.population.metadata.sources + land_area.land_area.metadata.sources,
    )


def _add_metadata(t: Table, population, land_area) -> Table:
    t.update_metadata_from_yaml(DIR_PATH / "key_indicators.meta.yml", "population_density")
    t.metadata.sources = _build_sources(population, land_area)
    return t


def make_table(ds: Dataset) -> Table:
    # Reset indices
    population = ds['population'].reset_index()
    land_area = ds['land_area'].reset_index()
    # Take the latest measurement of land area
    land_area = land_area.sort_values("year").groupby(["country"], as_index=False).last()
    # Combine tables
    t = _combine_tables(population, land_area)
    # Build metric
    t = _build_metric(t)
    # Sanity checks
    _sanity_checks(t)
    # Set index
    t = t.set_index(["country", "year"])
    # Add metadata
    t = _add_metadata(t, population, land_area)
    return t

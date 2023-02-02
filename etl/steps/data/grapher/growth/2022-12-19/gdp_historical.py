"""
Just upload GDP and GDP per capita estimations the way they are from Garden
"""
from owid import catalog

from etl.helpers import PathFinder

N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)

    table = N.garden_dataset["gdp_historical"]

    dataset.add(table)

    dataset.save()

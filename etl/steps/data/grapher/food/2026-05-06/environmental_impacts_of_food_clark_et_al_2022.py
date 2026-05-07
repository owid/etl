"""Grapher step for Clark et al. (2022) — pass-through from garden."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_garden = paths.load_dataset()
    tb = ds_garden["environmental_impacts_of_food_clark_et_al_2022"]

    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.save()

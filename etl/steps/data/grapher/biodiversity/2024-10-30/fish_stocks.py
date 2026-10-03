"""Grapher step for the RAM Legacy fish stocks dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_garden = paths.load_dataset("fish_stocks")
    tb = ds_garden.read("fish_stocks", reset_index=False)

    ds = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds.save()

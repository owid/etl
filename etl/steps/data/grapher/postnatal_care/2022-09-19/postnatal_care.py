"""Load garden dataset and create grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Load garden dataset and table.
    ds_garden = paths.load_dataset("postnatal_care")
    tb = ds_garden["postnatal_care"]

    # Save grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.save()

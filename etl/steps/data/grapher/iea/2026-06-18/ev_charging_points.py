"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("ev_charging_points")

    # Read table from garden dataset.
    tb = ds_garden.read("ev_charging_points", reset_index=False)

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.save()

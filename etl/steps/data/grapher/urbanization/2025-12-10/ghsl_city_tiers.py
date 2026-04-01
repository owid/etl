"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_garden = paths.load_dataset("ghsl_city_tiers")
    tb = ds_garden.read("ghsl_city_tiers", reset_index=False)
    ds_grapher = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata)
    ds_grapher.save()

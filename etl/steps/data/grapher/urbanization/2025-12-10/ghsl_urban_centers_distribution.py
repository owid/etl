"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_garden = paths.load_dataset("ghsl_urban_centers_distribution")
    tb = ds_garden.read("ghsl_urban_centers_distribution", reset_index=False)
    ds_grapher = paths.create_dataset(
        tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )
    ds_grapher.save()

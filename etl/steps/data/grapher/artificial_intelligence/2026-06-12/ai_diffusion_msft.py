"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_garden = paths.load_dataset("ai_diffusion_msft")
    tb = ds_garden.read("ai_diffusion_msft", reset_index=False)

    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata, check_variables_metadata=True)
    ds_grapher.save()

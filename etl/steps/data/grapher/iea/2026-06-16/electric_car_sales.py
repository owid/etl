"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("electric_car_sales")
    tb = ds_garden.read("electric_car_sales", reset_index=False)

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.save()

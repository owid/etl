"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("child_labor_report")

    # Read tables from garden dataset.
    tb = ds_garden.read("child_labor", reset_index=False)
    tb_sector = ds_garden.read("sector", reset_index=False)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb, tb_sector], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()

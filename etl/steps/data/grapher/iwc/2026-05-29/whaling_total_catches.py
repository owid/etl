"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("whaling_total_catches")

    # Read table from garden dataset.
    tb_total = ds_garden.read("whaling_total_catches", reset_index=False)
    tb_type = ds_garden.read("whaling_total_catches_type", reset_index=False)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb_total, tb_type], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()

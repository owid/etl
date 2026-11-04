"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("refugee_data")

    # Read table from garden dataset.
    tb_asylum = ds_garden.read("refugee_data_asylum", reset_index=False)
    tb_origin = ds_garden.read("refugee_data_origin", reset_index=False)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb_asylum, tb_origin], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()

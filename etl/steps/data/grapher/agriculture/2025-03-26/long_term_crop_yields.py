"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("long_term_crop_yields")
    tb = ds_garden.read("long_term_crop_yields", reset_index=False)

    # Load smoothed table.
    tb_smoothed = ds_garden.read("long_term_crop_yields_smoothed", reset_index=False)

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb, tb_smoothed], default_metadata=ds_garden.metadata)
    ds_grapher.save()

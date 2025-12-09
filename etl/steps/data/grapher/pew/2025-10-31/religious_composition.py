"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("religious_composition")

    # Read table from garden dataset.
    tbs = [
        ds_garden.read("religious_composition", reset_index=False),
        ds_garden.read(name="most_popular_religion", reset_index=False),
        ds_garden.read(name="share_change", reset_index=False),
    ]

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=tbs, default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()

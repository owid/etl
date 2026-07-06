"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read both tables (country-level, with the activity as a dimension).
    ds_garden = paths.load_dataset("emissions_by_human_activity")
    tb = ds_garden.read("emissions_by_human_activity", reset_index=False)
    tb_other = ds_garden.read("emissions_by_human_activity_including_other", reset_index=False)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb, tb_other], default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()

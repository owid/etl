"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its tables.
    ds_garden = paths.load_dataset("emissions_by_human_activity")
    tb = ds_garden.read("emissions_by_human_activity")
    tb_including_electricity = ds_garden.read("emissions_by_human_activity_including_electricity", reset_index=False)

    #
    # Prepare data.
    #
    # In the table where electricity is its own activity, the data is global, so use the sector as the entity.
    tb = tb.drop(columns=["country"]).rename(columns={"sector": "country"}, errors="raise")
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb, tb_including_electricity], default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()

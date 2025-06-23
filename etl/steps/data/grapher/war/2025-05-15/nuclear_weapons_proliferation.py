"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its tables.
    ds_garden = paths.load_dataset("nuclear_weapons_proliferation")
    tb = ds_garden.read("nuclear_weapons_proliferation", reset_index=False)
    tb_counts = ds_garden.read("nuclear_weapons_proliferation_counts", reset_index=False)

    #
    # Process data.
    #
    # Add a country column to the index of the table of counts.
    tb_counts = tb_counts.reset_index().assign(**{"country": "World"}).format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb, tb_counts], default_metadata=ds_garden.metadata)
    ds_grapher.save()

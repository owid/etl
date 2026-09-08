"""Load the garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("us_lawful_permanent_residents")

    tables = [
        ds_garden.read("us_lawful_permanent_residents", reset_index=False),
        ds_garden.read("by_country_of_origin", reset_index=False),
        ds_garden.read("by_region_of_origin", reset_index=False),
    ]

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=tables, default_metadata=ds_garden.metadata)
    ds_grapher.save()

"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    paths.log.info("semiconductors_cset.start")

    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("semiconductors_cset")

    # Read table from garden dataset.
    tb = ds_garden.read("semiconductors_cset")

    #
    # Process data.
    #
    tb = tb.rename(columns={"provider": "country"})
    # Format the table
    tb = tb.format(["country", "year", "provided_name"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()

    paths.log.info("semiconductors_cset.end")

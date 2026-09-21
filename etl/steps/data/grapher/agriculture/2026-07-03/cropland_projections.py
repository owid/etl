"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("cropland_projections")

    # Read table from garden dataset.
    tb = ds_garden.read("cropland_projections")

    #
    # Process data.
    #
    # Remove the 2012 base year, so that, in charts, projections start in 2030 and do not overlap with the observed
    # data (which could otherwise be misread as two competing versions of the recent past).
    tb = tb[tb["year"] > 2012].reset_index(drop=True)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()

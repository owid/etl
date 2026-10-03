"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("divorces")

    # Read table from garden dataset.
    tb = ds_garden.read("divorces", reset_index=True)

    tb = tb.rename(columns={"year": "year_of_marriage", "anniversary_year": "year"})

    tb = tb.format(["country", "year_of_marriage", "year"])
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb], check_variables_metadata=True)

    # Save changes in the new grapher dataset.
    ds_grapher.save()

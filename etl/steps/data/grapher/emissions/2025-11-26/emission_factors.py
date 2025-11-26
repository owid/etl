"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("emission_factors")

    # Read table from garden dataset.
    tb = ds_garden.read("emission_factors")

    #
    # Process data.
    #
    # For compatibility with grapher, add a country and year column.
    tb = tb.rename(columns={"fuel": "country"}, errors="raise")
    tb["year"] = paths.version.split("-")[0]

    # Improve table format.
    tb = tb.format(keys=["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()

"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("acceptability_of_uk_farming_practices")

    # Read table from garden dataset.
    tb = ds_garden.read("acceptability_of_uk_farming_practices")

    #
    # Process data.
    #
    # Adapt to grapher format.
    tb = tb.rename(columns={"question": "country"})
    # Add a year column (take it from the origin's publication date).
    tb["year"] = tb["acceptable"].metadata.origins[0].date_published.split("-")[0]

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()

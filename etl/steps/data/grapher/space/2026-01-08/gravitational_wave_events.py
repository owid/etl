"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("gravitational_wave_events")

    # Read table from garden dataset.
    tb = ds_garden.read("gravitational_wave_events")

    #
    # Process data.
    #
    # Add a country column, to adapt to grapher.
    tb["country"] = "World"

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()

"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("global_historical_electricity_scaling")

    # Read table from garden dataset.
    tb = ds_garden.read("global_historical_electricity_scaling")

    #
    # Process data.
    #
    # Use sources as entities (replacing the "World" country column).
    tb = tb.drop(columns=["country"]).rename(columns={"source": "country"}, errors="raise")

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()

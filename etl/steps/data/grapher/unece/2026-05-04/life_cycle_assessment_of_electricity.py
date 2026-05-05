"""Load garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("life_cycle_assessment_of_electricity")
    tb = ds_garden.read("life_cycle_assessment_of_electricity")

    #
    # Process data.
    #
    # Adapt to grapher format.
    tb = tb.rename(columns={"source": "country"}, errors="raise")

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.save()

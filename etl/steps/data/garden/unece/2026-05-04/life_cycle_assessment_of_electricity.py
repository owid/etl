"""Garden step for UNECE's Life Cycle Assessment of Electricity Generation Options."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("life_cycle_assessment_of_electricity")
    tb = ds_meadow.read("life_cycle_assessment_of_electricity", reset_index=False)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()

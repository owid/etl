"""Grapher step for the historical Ebola outbreak chronology (annual, by country)."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("ebola_outbreaks")
    tb = ds_garden["ebola_outbreaks"]

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.save()

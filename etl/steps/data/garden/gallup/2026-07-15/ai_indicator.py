"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ai_indicator")
    tb = ds_meadow.read("ai_indicator")

    # US-only survey.
    tb["country"] = "United States"

    tb = tb.format(["country", "date"])

    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)
    ds_garden.save()

"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot("health_pharma_market.csv")
    tb = snap.read_csv(underscore=True)
    tb = tb.format(["country", "year", "variable", "measure"], short_name=paths.short_name)

    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()

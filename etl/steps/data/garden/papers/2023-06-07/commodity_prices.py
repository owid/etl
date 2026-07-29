"""Load snapshot and create a garden dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot()
    tb = snap.read_excel(skiprows=1).rename(columns={"(1900=100)": "year"})
    tb["country"] = "World"

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    ds_garden = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_garden.save()

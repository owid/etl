"""Load snapshot and create a garden dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot()
    tb = snap.read_excel(skiprows=1).rename(columns={"(1900=100)": "year"})
    tb["country"] = "World"

    # TEMPORARY: deliberate failure, to check what a failed step looks like in the Buildkite log.
    # Reverted before this PR is merged. Raises inside pandas, so the traceback has several frames.
    tb["country"] = tb["country"].astype(float)

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    ds_garden = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_garden.save()

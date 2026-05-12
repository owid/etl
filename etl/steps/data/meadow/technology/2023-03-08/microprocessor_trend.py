"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot("microprocessor_trend.dat")
    tb = snap.read_csv(
        sep=r"\s+",
        header=None,
        names=["year", "transistors"],
        comment="#",
    )

    tb["region"] = "World"

    # The raw file has multiple rows per year (different chips), so we keep them all
    # and defer aggregation to garden. Index by the raw float year + a synthetic row id.
    tb = tb.reset_index().rename(columns={"index": "row_id"})
    tb = tb.format(["row_id"], short_name=paths.short_name)

    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()

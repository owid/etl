"""Meadow step: light type cleanup; preserves the snapshot's row shape."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot("nvidia_revenue.csv")
    tb = snap.read()

    # Cast low-cardinality string columns to categorical for compactness.
    for col in ["source_pdf", "quarter", "segment"]:
        tb[col] = tb[col].astype("category")

    tb = tb.format(["source_pdf", "quarter", "segment"])

    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)
    ds_meadow.save()

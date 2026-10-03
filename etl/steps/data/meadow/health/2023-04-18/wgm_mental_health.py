"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot("wgm_mental_health.zip")

    # Read CSV from inside the zip; snap.read_csv returns a Table with origins on every column.
    tb = snap.read_csv(compression="zip", underscore=True)

    # Cast all non-weight columns to string (questions are categorical IDs).
    tb = tb.astype({col: str for col in tb.columns if col not in ["wgt", "projwt"]})

    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()

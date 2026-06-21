"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

COLUMNS_TO_KEEP = ["Reference area", "TIME_PERIOD", "OBS_VALUE"]
COLUMN_RENAMES = {"Reference area": "country", "TIME_PERIOD": "year", "OBS_VALUE": "share_tertiary_education"}


def run() -> None:
    snap = paths.load_snapshot("education_attainment_distribution.csv")
    tb = snap.read(safe_types=False)

    # Keep only relevant columns.
    tb = tb[COLUMNS_TO_KEEP]

    # Rename columns.
    tb = tb.rename(columns=COLUMN_RENAMES, errors="raise")

    # Drop rows without data.
    tb = tb.dropna(subset=["share_tertiary_education"])

    # Use categoricals for country column (remove unused categories after dropna).
    tb["country"] = tb["country"].astype(str).astype("category").cat.remove_unused_categories()

    tb = tb.format(["country", "year"])

    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot("flu_elderly.xlsx")
    tb = snap.read_excel()

    # Drop the trailing summary line (all-NA).
    tb = tb[:-1]

    # Keep relevant columns and drop rows without coverage.
    tb = tb[["NAME", "YEAR", "DOSES", "COVERAGE"]].dropna(subset="COVERAGE")
    tb = tb.rename(columns={"NAME": "country", "YEAR": "year"})

    tb = tb.format(["country", "year"], short_name=paths.short_name)
    tb.update_metadata_from_yaml(paths.metadata_path, "flu_elderly")

    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()

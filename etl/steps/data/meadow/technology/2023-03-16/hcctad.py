"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot("hcctad.txt")
    tb = snap.read_csv(na_values=".")

    tb = (
        tb.rename(columns={"Variable": "variable", "Year": "year"})
        .melt(["variable", "year"], var_name="country", value_name="value")
        .dropna()
        .reset_index(drop=True)
    )

    tb = tb.format(["country", "variable", "year"], short_name=paths.short_name)

    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()

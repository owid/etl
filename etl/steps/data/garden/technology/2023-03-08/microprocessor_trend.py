"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_meadow = paths.load_dataset("microprocessor_trend")
    tb = ds_meadow.read("microprocessor_trend")

    # Transistors are counted in thousands.
    tb["transistors"] = tb["transistors"] * 1000

    # Sort chronologically; keep highest-ever count.
    tb = tb.sort_values("year")
    tb["transistors"] = tb["transistors"].cummax()

    # Trim to years and keep the yearly max.
    tb["year"] = tb["year"].astype(int)
    tb = tb[["year", "region", "transistors"]]
    tb = tb.groupby(["year", "region"], as_index=False).max()

    tb = tb.format(["year", "region"], short_name=paths.short_name)

    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()

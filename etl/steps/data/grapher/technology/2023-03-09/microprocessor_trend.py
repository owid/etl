"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_garden = paths.load_dataset("microprocessor_trend")
    tb = ds_garden.read("microprocessor_trend").rename(columns={"region": "country"})
    tb = tb.format(["year", "country"], short_name=paths.short_name)

    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.save()

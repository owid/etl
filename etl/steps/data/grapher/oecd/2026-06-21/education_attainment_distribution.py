"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_garden = paths.load_dataset("education_attainment_distribution")
    tb = ds_garden["education_attainment_distribution"].reset_index()

    # Remove non-country aggregates that don't map to grapher entities.
    aggregates_to_exclude = ["European Union (25 countries)", "G20", "OECD"]
    tb = tb[~tb["country"].isin(aggregates_to_exclude)]

    tb = tb.format(["country", "year"])

    ds_grapher = paths.create_dataset(tables=[tb])
    ds_grapher.save()

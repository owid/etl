"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, grapher_checks

paths = PathFinder(__file__)


def run() -> None:
    ds_garden = paths.load_dataset("gender_statistics_country_counts")
    tb = ds_garden.read("gender_statistics", reset_index=False)

    ds_grapher = paths.create_dataset(tables=[tb])
    grapher_checks(ds_grapher)
    ds_grapher.save()

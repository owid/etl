from owid.catalog import Dataset

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SOURCE_NAME = "gapminder_v7"


def load_gapminder():
    ds: Dataset = paths.load_dependency(short_name="population", namespace="gapminder")
    tb = ds["population"]
    tb["source"] = SOURCE_NAME
    tb = tb.reset_index()
    return tb

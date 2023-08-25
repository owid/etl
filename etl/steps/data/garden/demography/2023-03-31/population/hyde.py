from owid.catalog import Dataset

from etl.helpers import PathFinder

SOURCE_NAME = "hyde"


# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def load_hyde():
    ds: Dataset = paths.load_dependency("baseline")
    tb = ds["population"]
    tb["source"] = SOURCE_NAME
    tb = tb.reset_index()
    return tb

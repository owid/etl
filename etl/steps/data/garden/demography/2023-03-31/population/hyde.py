from etl.helpers import PathFinder

SOURCE_NAME = "hyde"


# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def load_hyde():
    ds = paths.load_dataset("baseline")
    tb = ds["population"]
    tb["source"] = SOURCE_NAME
    tb = tb.reset_index()
    return tb

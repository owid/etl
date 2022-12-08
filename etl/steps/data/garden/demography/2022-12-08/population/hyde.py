from owid.catalog import Dataset

from etl.paths import DATA_DIR

DATASET_HYDE = DATA_DIR / "garden" / "hyde" / "2017" / "baseline"
SOURCE_NAME = "hyde"


def load_gapminder():
    tb = Dataset(DATASET_HYDE)["population"]
    tb["source"] = SOURCE_NAME
    tb = tb.reset_index()
    return tb

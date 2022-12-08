from owid.catalog import Dataset

from etl.paths import DATA_DIR

DATASET_GAPMINDER = DATA_DIR / "garden" / "gapminder" / "2019-12-10" / "population"
SOURCE_NAME = "gapminder"


def load_gapminder():
    tb = Dataset(DATASET_GAPMINDER)["population"]
    tb["source"] = SOURCE_NAME
    tb = tb.reset_index()
    return tb

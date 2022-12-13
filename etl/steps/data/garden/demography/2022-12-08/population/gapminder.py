from owid.catalog import Dataset

from etl.paths import DATA_DIR

DATASET_GAPMINDER = DATA_DIR / "garden" / "gapminder" / "2019-12-10" / "population"
DATASET_GAPMINDER_SYSTEMA_GLOBALIS = (
    DATA_DIR / "open_numbers" / "open_numbers" / "latest" / "gapminder__systema_globalis"
)
SOURCE_NAME = "gapminder"


def load_gapminder():
    tb = Dataset(DATASET_GAPMINDER)["population"]
    tb["source"] = SOURCE_NAME
    tb = tb.reset_index()
    return tb

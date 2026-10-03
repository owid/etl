#
#  table_population.py
#  key_indicators
#

"""
Adapted from Ed's importers script:

https://github.com/owid/importers/blob/master/population/etl.py
"""

from pathlib import Path

from owid.catalog import Dataset, Table

from etl.paths import DATA_DIR

# OMM population dataset
OMM_POPULATION = DATA_DIR / "garden/demography/2022-12-08/population"
DIR_PATH = Path(__file__).parent
# Countries to exclude (to be reviewed)
EXCLUDE_COUNTRIES = [
    "Czechoslovakia",
    "East Germany",
    "Ethiopia (former)",
    "Serbia and Montenegro",
    "USSR",
    "West Germany",
    "Yemen Arab Republic",
    "Yemen People's Republic",
    "Yugoslavia",
    "Akrotiri and Dhekelia",
]


def make_table() -> Table:
    # Load table from OMM dataset
    ds = Dataset(OMM_POPULATION)
    tb = ds["population"]

    # drop unneded columns
    tb = tb.drop(columns=["source"])

    # reset index
    tb = tb.reset_index()

    # Remove former countries (this is to ensure that the current key_indicators 'population' table is not changed)
    msk = tb["country"].isin(EXCLUDE_COUNTRIES)
    tb = tb.loc[~msk]
    # ensure categories not in use are removed
    tb["country"] = tb["country"].cat.remove_unused_categories()

    tb = tb.set_index(["country", "year"])
    return tb


if __name__ == "__main__":
    t = make_table()

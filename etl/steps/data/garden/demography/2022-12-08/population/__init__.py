from copy import deepcopy
from typing import List

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.helpers import Names
from etl.paths import DATA_DIR

from .gapminder import load_gapminder
from .hyde import load_hyde
from .unwpp import load_unwpp

log = get_logger()

# naming conventions
N = Names(__file__)


# dataset paths
DATASET_GAPMINDER = DATA_DIR / "garden" / "gapminder" / "2019-12-10" / "population"
DATASET_HYDE = DATA_DIR / "garden" / "hyde" / "2017" / "baseline"
DATASET_WBINCOME = DATA_DIR / "garden" / "wb" / "2021-07-01" / "wb_income"
# exclude countries
EXCLUDE_COUNTRIES_UNWPP = N.directory / "exclude_countries.unwpp.json"


def run(dest_dir: str) -> None:
    log.info("population.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/demography/2022-12-08/population")
    tb_meadow = ds_meadow["population"]

    df = pd.DataFrame(tb_meadow)

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    tb_garden = underscore_table(Table(df))
    tb_garden.metadata = tb_meadow.metadata
    for col in tb_garden.columns:
        tb_garden[col].metadata = tb_meadow[col].metadata

    ds_garden.metadata.update_from_yaml(N.metadata_path)
    tb_garden.update_metadata_from_yaml(N.metadata_path, "population")

    ds_garden.add(tb_garden)
    ds_garden.save()

    log.info("population.end")


def load_data() -> pd.DataFrame:
    """Load data from all sources and concatenate them into a single dataframe."""
    unwpp = load_unwpp()
    gapminder = load_gapminder()
    hyde = load_hyde()
    tb = pd.DataFrame(pd.concat([gapminder, hyde, unwpp], ignore_index=True))
    return tb

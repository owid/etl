import pandas as pd
from owid import catalog
from owid.catalog import Dataset
from owid.catalog.utils import underscore
from owid.datautils import dataframes
from structlog import get_logger

from etl.paths import DATA_DIR, REFERENCE_DATASET

log = get_logger()

from .shared import CURRENT_DIR

METADATA_PATH = CURRENT_DIR / "overview.meta.yml"


WHO_MORTALITY_DB = DATA_DIR / "garden/homicide/2023-01-03/who_mort_db"


def get_who_mortality_db() -> pd.DataFrame:
    """
    Get the homicide rate from the WHO Mortality Database Dataset
    """
    ds_who_db = catalog.Dataset(WHO_MORTALITY_DB)
    who_md = ds_who_db["who_mort_db"].reset_index()
    who_md = pd.DataFrame(who_md[["country", "year", "death_rate_per_100_000_population"]])
    who_md = who_md.dropna(subset="death_rate_per_100_000_population")
    return who_md


def get_backports() -> pd.DataFrame:
    """
    Get the i) Child mortality rate, ii) Electoral democracy, iii) Homicide rate,
    iv) Average years of schooling, v) Life expectancy and vi) Daily supply of calories per person
    from the backported datasets.
    """
    base_path = DATA_DIR / "backport/owid/latest/"

    # list of all backports to include, map from dataset name to list of variables to include
    backports = {
        "dataset_2710_child_mortality_rates__selected_gapminder__v10__2017": [
            "child_mortality__select_gapminder__v10__2017"
        ],
    }
    # make one mega table with all variables from all the backports
    t_all = pd.DataFrame()

    for dataset, variables in backports.items():
        log.info(f"Fetching the backport of... {dataset}")
        ds = catalog.Dataset(f"{base_path}/{dataset}")
        t = ds[dataset]

        # assert variables are in the table - if not throw an error
        # fix the index to be (year, entity_name)
        t = t.reset_index().drop(columns=["entity_id", "entity_code"]).set_index(["entity_name", "year"])[variables]

        if t_all.shape == tuple([0, 0]):
            # first time around
            t_all = t
        else:
            t_all = t_all.join(t, how="outer")  # omg hope
    t_all = t_all.reset_index()
    t_all = t_all.rename(columns={"entity_name": "country"})
    return t_all

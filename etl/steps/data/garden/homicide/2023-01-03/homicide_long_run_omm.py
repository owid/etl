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

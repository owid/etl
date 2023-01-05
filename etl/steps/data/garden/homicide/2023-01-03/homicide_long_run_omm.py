import pandas as pd
from owid import catalog
from structlog import get_logger

from etl.helpers import PathFinder
from etl.paths import DATA_DIR

log = get_logger()


WHO_MORTALITY_DB = DATA_DIR / "garden/homicide/2023-01-03/who_mort_db"
paths = PathFinder(__file__)

ds_eisner: catalog.Dataset = paths.load_dependency("long_term_homicide_rates_in_europe")


def get_who_mortality_db() -> pd.DataFrame:
    """
    Get the homicide rate from the WHO Mortality Database Dataset
    """
    ds_who_db = catalog.Dataset(WHO_MORTALITY_DB)
    who_md = ds_who_db["who_mort_db"].reset_index()
    who_md = pd.DataFrame(who_md[["country", "year", "death_rate_per_100_000_population"]])
    who_md = who_md.dropna(subset="death_rate_per_100_000_population")
    return who_md

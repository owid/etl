import json
from typing import List, cast

import pandas as pd

from etl.helpers import Names

# naming conventions
N = Names(__file__)


def load_excluded_countries(excluded_countries_path: str) -> List[str]:
    with open(excluded_countries_path, "r") as f:
        data = json.load(f)
        assert isinstance(data, list)
    return data


def exclude_countries(df: pd.DataFrame, excluded_countries_path: str) -> pd.DataFrame:
    excluded_countries = load_excluded_countries(excluded_countries_path)
    return cast(pd.DataFrame, df.loc[~df.country.isin(excluded_countries)])

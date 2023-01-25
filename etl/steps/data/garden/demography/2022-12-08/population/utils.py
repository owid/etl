import json
from pathlib import Path
from typing import List, cast

import pandas as pd

from etl.helpers import PathFinder

# naming conventions
N = PathFinder(__file__)


def load_excluded_countries(excluded_countries_path: Path) -> List[str]:
    with open(excluded_countries_path, "r") as f:
        data = json.load(f)
        assert isinstance(data, list)
    return data


def exclude_countries(df: pd.DataFrame, excluded_countries_path: Path) -> pd.DataFrame:
    excluded_countries = load_excluded_countries(excluded_countries_path)
    return cast(pd.DataFrame, df.loc[~df.country.isin(excluded_countries)])

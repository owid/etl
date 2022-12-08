import json
from typing import List, cast

import pandas as pd

from owid.datautils import geo

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


def add_regions(df: pd.DataFrame) -> pd.DataFrame:
    """Add continents and income groups."""
    regions = [
        "Europe",
        "Asia",
        "North America",
        "South America",
        "Africa",
        "Oceania",
        "High-income countries",
        "Low-income countries",
        "Lower-middle-income countries",
        "Upper-middle-income countries",
        "European Union (27)",
    ]
    df = df.loc[-df.country.isin(regions)]
    for region in regions:
        df = geo.add_region_aggregates(df=df, region=region)
    return df

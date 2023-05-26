"""Utils for key_indicators module."""
import pandas as pd

from etl.data_helpers import geo


def add_regions(df: pd.DataFrame, population: pd.DataFrame) -> pd.DataFrame:
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
        # TODO: this should be ideally
        # countries_in_region = geo.list_members_of_region(region=region, ds_regions=ds_regions, ds_income_groups=ds_income_groups)
        # df = geo.add_region_aggregates(df=df, region=region, countries_in_region=countries_in_region, population=population)
        df = geo.add_region_aggregates(df=df, region=region, population=population)
    return df

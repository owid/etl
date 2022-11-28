"""Utils for key_indicators module."""
import pandas as pd
from owid.datautils import geo


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
    for region in regions:
        df = geo.add_region_aggregates(df=df, region=region)
    return df

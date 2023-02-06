import json
from pathlib import Path
from typing import Optional, Union

import pandas as pd

from etl.data_helpers import geo
from etl.data_helpers.misc import check_values_in_column


def harmonize_countries(
    df: pd.DataFrame,
    country_column: str,
    countries_file: Union[str, Path],
    countries_exclude_file: Optional[Union[str, Path]] = None,
) -> pd.DataFrame:
    # Load country mapping file
    with open(countries_file) as f:
        country_mapping = json.load(f)
    # Check values in column
    check_values_in_column(df, country_column, list(country_mapping.keys()))
    # Harmonize country names
    df = geo.harmonize_countries(
        df=df,
        countries_file=countries_file,
        excluded_countries_file=countries_exclude_file,
        country_col=country_column,
        warn_on_missing_countries=True,
        warn_on_unused_countries=True,
    ).rename(columns={country_column: "entity"})
    return df

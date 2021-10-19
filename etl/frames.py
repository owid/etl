#
#  frames.py
#  etl
#

from typing import List, cast

import numpy as np
import pandas as pd
from pandas.core import series


def repack_frame(df: pd.DataFrame) -> pd.DataFrame:
    if len(df.index.names) == 1 and not df.index.names[0]:
        primary_key = []
    else:
        primary_key = cast(List[str], df.index.names)
        df.reset_index(inplace=True)

    for col in df.columns:
        df[col] = repack_series(df[col])

    if primary_key:
        df.set_index(primary_key, inplace=True)

    return df


def repack_series(s: pd.Series) -> pd.Series:
    # only repack object columns
    if s.dtype not in (np.object_, np.float64):
        return s

    for type_ in ["Int64", "float64", "category"]:
        try:
            v = cast(pd.Series, s.astype(type_))
        except (ValueError, TypeError):
            continue

        if series_eq(v, s):
            # successful repack
            return v

    return s


def series_eq(lhs: pd.Series, rhs: pd.Series):
    """
    Check that series are equal, but unlike normal floating point checks where
    NaN != NaN, we want missing or null values to be reported as equal to each
    other.
    """
    return (
        len(lhs) == len(rhs)
        and (lhs.isnull() == rhs.isnull()).all()
        and (lhs.dropna() == rhs.dropna()).all()
    )

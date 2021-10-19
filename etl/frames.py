#
#  frames.py
#  etl
#

from typing import Any, Dict, List, Union, cast

import numpy as np
import pandas as pd


def repack_frame(df: pd.DataFrame, remap: Dict[str, str]) -> None:
    """
    Convert the DataFrame's columns to the most compact types possible.
    Rename columns if necessary during the repacking. The column renames
    work even if the column is part of the index.
    """
    if len(df.index.names) == 1 and not df.index.names[0]:
        primary_key = []
    else:
        primary_key = cast(List[str], df.index.names)
        df.reset_index(inplace=True)

    for col in df.columns:
        df[col] = repack_series(df[col])

    for from_, to_ in remap.items():
        if from_ in df.columns:
            df.rename(columns={from_: to_}, inplace=True)
    primary_key = [remap.get(k, k) for k in primary_key]

    assert all(df[col].dtype != "object" for col in df.columns)

    if primary_key:
        df.set_index(primary_key, inplace=True)


def repack_series(s: pd.Series) -> pd.Series:
    # only repack object columns
    if s.dtype not in (np.object_, np.float64):
        return s

    for strategy in [to_int, to_float, to_category]:
        try:
            return strategy(s)
        except (ValueError, TypeError):
            continue

    return s


def to_int(s: pd.Series) -> pd.Series:
    # values could be integers or strings
    def intify(v: Any) -> Union[int, None]:
        return int(v) if not pd.isnull(v) else None

    v = cast(pd.Series, s.apply(intify).astype("Int64"))

    if not series_eq(v, s, cast=float):
        raise ValueError()

    return v


def to_float(s: pd.Series) -> pd.Series:
    v = cast(pd.Series, s.astype("float64"))

    if not series_eq(v, s, cast=float):
        raise ValueError()

    return v


def to_category(s: pd.Series) -> pd.Series:
    types = set(s.apply(type).unique())

    if types.difference({str, type(None)}):
        raise ValueError()

    return cast(pd.Series, s.astype("category"))


def series_eq(lhs: pd.Series, rhs: pd.Series, cast: Any) -> bool:
    """
    Check that series are equal, but unlike normal floating point checks where
    NaN != NaN, we want missing or null values to be reported as equal to each
    other.
    """
    return (
        len(lhs) == len(rhs)
        and (lhs.isnull() == rhs.isnull()).all()
        and (lhs.dropna().apply(cast) == rhs.dropna().apply(cast)).all()
    )

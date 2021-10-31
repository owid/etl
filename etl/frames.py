#
#  frames.py
#  etl
#

from typing import Any, Dict, List, Optional, Union, cast

import numpy as np
import pandas as pd


def repack_frame(df: pd.DataFrame, remap: Optional[Dict[str, str]] = None) -> None:
    """
    Convert the DataFrame's columns to the most compact types possible.
    Rename columns if necessary during the repacking. The column renames
    work even if the column is part of the index.
    """
    remap = remap or {}

    # unwind the primary key
    if len(df.index.names) == 1 and not df.index.names[0]:
        primary_key = []
    else:
        primary_key = cast(List[str], df.index.names)
        df.reset_index(inplace=True)

    # repack each column into the best dtype we can give it
    for col in df.columns:
        df[col] = repack_series(df[col])

    # remap all column names, including those in the primary key
    for from_, to_ in remap.items():
        if from_ in df.columns:
            df.rename(columns={from_: to_}, inplace=True)
    primary_key = [remap.get(k, k) for k in primary_key]

    assert all(df[col].dtype != "object" for col in df.columns)

    # set the primary key back again
    if primary_key:
        df.set_index(primary_key, inplace=True)


def repack_series(s: pd.Series) -> pd.Series:
    if s.dtype.name in ("Int64", "int64"):
        return shrink_integer(s)

    if s.dtype.name in ("object", "float64", "Float64"):
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

    # it's an integer, now pack it smaller
    return shrink_integer(v)


def shrink_integer(s: pd.Series) -> pd.Series:
    """
    Take an Int64 series and make it as small as possible.
    """
    assert s.dtype.name in ("Int64", "int64")

    if s.isnull().any():
        if s.min() < 0:
            series = ["Int32", "Int16", "Int8"]
        else:
            series = ["UInt32", "UInt16", "UInt8"]
    else:
        if s.min() < 0:
            series = ["int32", "int16", "int8"]
        else:
            series = ["uint32", "uint16", "uint8"]

    for dtype in series:
        v = s.astype(dtype)
        if not (v == s).all():
            break

        s = v

    return s


def to_float(s: pd.Series) -> pd.Series:
    options = ["float32", "float64"]
    for dtype in options:
        v = s.astype(dtype)

        if series_eq(s, v, float):
            return v

    raise ValueError()


def to_category(s: pd.Series) -> pd.Series:
    types = set(s.apply(type).unique())

    if types.difference({str, type(None)}):
        raise ValueError()

    return cast(pd.Series, s.astype("category"))


def series_eq(
    lhs: pd.Series, rhs: pd.Series, cast: Any, rtol: float = 1e-5, atol: float = 1e-8
) -> bool:
    """
    Check that series are equal, but unlike normal floating point checks where
    NaN != NaN, we want missing or null values to be reported as equal to each
    other.
    """
    if len(lhs) != len(rhs) or (lhs.isnull() != rhs.isnull()).all():
        return False

    lhs_values = lhs.dropna().apply(cast)
    rhs_values = rhs.dropna().apply(cast)
    return np.allclose(lhs_values, rhs_values, rtol=rtol, atol=atol)

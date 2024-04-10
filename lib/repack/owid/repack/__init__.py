import datetime as dt
from typing import Any, Dict, List, Optional, cast

import numpy as np
import pandas as pd


def repack_frame(
    df: pd.DataFrame,
    remap: Optional[Dict[str, str]] = None,
    dtypes: Optional[Dict[str, Any]] = {},
) -> pd.DataFrame:
    """
    Convert the DataFrame's columns to the most compact types possible.
    Rename columns if necessary during the repacking. The column renames
    work even if the column is part of the index.

    :param remap: remap column names
    :param dtypes: dictionary of fixed dtypes to use
    """
    if df.index.names != [None]:
        raise ValueError("repacking is lost for index columns")

    remap = remap or {}
    dtypes = dtypes or {}

    # unwind the primary key
    if len(df.index.names) == 1 and not df.index.names[0]:
        primary_key = []
    else:
        primary_key = cast(List[str], df.index.names)
        df.reset_index(inplace=True)

    # repack each column into the best dtype we can give it
    df = pd.concat(
        [repack_series(df[col]) if col not in dtypes else df[col] for col in df.columns],
        axis=1,
    )

    # use given dtypes
    if dtypes:
        df = df.astype(dtypes)

    # remap all column names, including those in the primary key
    for from_, to_ in remap.items():
        if from_ in df.columns:
            df.rename(columns={from_: to_}, inplace=True)
    primary_key = [remap.get(k, k) for k in primary_key]

    for col in df.columns:
        if df[col].dtype == "object":
            raise ValueError(f"Column {col} is still object. Consider converting it to str.")

    # set the primary key back again
    if primary_key:
        df.set_index(primary_key, inplace=True)

    return df


def repack_series(s: pd.Series) -> pd.Series:
    if s.dtype.name in ("Int64", "int64", "UInt64", "uint64"):
        return shrink_integer(s)

    if s.dtype.name in ("object", "float64", "Float64"):
        for strategy in [to_int, to_float, to_category]:
            try:
                return strategy(s)
            except (ValueError, TypeError, OverflowError):
                continue

    return s


def to_int(s: pd.Series) -> pd.Series:
    # values could be integers or strings
    v = s.astype("float64").astype("Int64")

    if not series_eq(v, s, cast=float):
        raise ValueError()

    # it's an integer, now pack it smaller
    return shrink_integer(v)


def shrink_integer(s: pd.Series) -> pd.Series:
    """
    Take an Int64 series and make it as small as possible.
    """
    assert s.dtype.name in ("Int64", "int64", "UInt64", "uint64")

    if s.isnull().all():
        # shrink all NaNs to Int8
        return s.astype("Int8")
    elif s.isnull().any():
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
    types = set(s.dropna().apply(type).unique())

    if types.difference({str, np.str_, dt.datetime, dt.date, type(None)}):
        raise ValueError()

    return s.astype("category")


def series_eq(lhs: pd.Series, rhs: pd.Series, cast: Any, rtol: float = 1e-5, atol: float = 1e-8) -> bool:
    """
    Check that series are equal, but unlike normal floating point checks where
    NaN != NaN, we want missing or null values to be reported as equal to each
    other.
    """
    # NOTE: this could be speeded up with numpy methods or smarter comparison,
    # but it's not bottleneck at the moment
    if len(lhs) != len(rhs):
        return False

    # improve performance by calling native astype method
    if cast == float:
        func = lambda s: s.astype(float)  # noqa: E731
    else:
        # NOTE: this would be extremely slow in practice
        func = lambda s: s.apply(cast)  # noqa: E731

    return np.allclose(func(lhs), func(rhs), rtol=rtol, atol=atol, equal_nan=True)

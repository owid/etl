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
        [repack_series(df.loc[:, col]) if col not in dtypes else df[col] for col in df.columns],
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
    dtype_name = s.dtype.name.replace("[pyarrow]", "").replace("[pyarrow_numpy]", "").lower()

    if dtype_name in ("int64", "uint64"):
        return shrink_integer(s.astype("Int64"))

    if dtype_name in ("object", "str", "string", "float64"):
        for strategy in [to_int, to_float, to_category]:
            try:
                return strategy(s)
            except (ValueError, TypeError, OverflowError):
                continue

    return s


def _to_float(s: pd.Series) -> pd.Series:
    """Convert series to Float64. Replace numpy NaNs with NA. This can
    happen when original series is an object and contains 'nan' string."""
    r = s.astype("Float64")
    if s.dtype == "object":
        r = r.mask(np.isnan(r), pd.NA)
    return r


def to_int(s: pd.Series) -> pd.Series:
    # values could be integers or strings
    s = _to_float(s)
    v = s.astype("Int64")

    # casting to float converts strings to floats, that doesn't work with float64[pyarrow]
    if not series_eq(v, s):
        raise ValueError()

    # it's an integer, now pack it smaller
    return shrink_integer(v)


def shrink_integer(s: pd.Series) -> pd.Series:
    """
    Take an Int64 series and make it as small as possible.
    """
    assert s.dtype == "Int64"

    if s.isnull().all():
        # shrink all NaNs to Int8
        return s.astype("Int8")
    else:
        if s.min() < 0:
            series = ["Int32", "Int16", "Int8"]
        else:
            series = ["UInt32", "UInt16", "UInt8"]

    for dtype in series:
        v = s.astype(dtype)
        if not (v == s).all():
            break

        s = v

    return s


def to_float(s: pd.Series) -> pd.Series:
    return shrink_float(_to_float(s))


def shrink_float(s: pd.Series) -> pd.Series:
    """
    Take a Float64 series and make it as small as possible.
    """
    assert s.dtype.name.replace("[pyarrow]", "") in ("float64", "Float64", "double"), s.dtype

    options = ["Float32", "Float64"]
    for dtype in options:
        v = s.astype(dtype)

        if series_eq(s, v):
            return v

    raise ValueError()


def to_category(s: pd.Series) -> pd.Series:
    types = set(s.dropna().apply(type).unique())

    if types.difference({str, np.str_, dt.datetime, dt.date, type(None)}):
        raise ValueError()

    return s.astype("category")


def series_eq(lhs: pd.Series, rhs: pd.Series, rtol: float = 1e-5, atol: float = 1e-8) -> bool:
    """
    Check that series are equal, but unlike normal floating point checks where
    NaN != NaN, we want missing or null values to be reported as equal to each
    other.
    """
    # NOTE: this could be speeded up with numpy methods or smarter comparison,
    # but it's not bottleneck at the moment
    if len(lhs) != len(rhs):
        return False

    return np.allclose(lhs, rhs, rtol=rtol, atol=atol, equal_nan=True)


def _safe_dtype(dtype: Any) -> str:
    """Determine the appropriate dtype string based on pandas dtype."""
    if pd.api.types.is_integer_dtype(dtype):
        return "Int64"
    elif pd.api.types.is_float_dtype(dtype):
        return "Float64"
    elif pd.api.types.is_bool_dtype(dtype):
        return "boolean"
    elif isinstance(dtype, pd.CategoricalDtype):
        return "string[pyarrow]"
    elif dtype == "object":
        return "string[pyarrow]"
    else:
        return dtype


def to_safe_types(t: pd.DataFrame) -> pd.DataFrame:
    """Convert numeric columns to Float64 and Int64 and categorical
    columns to string[pyarrow]."""
    t = t.astype({col: _safe_dtype(t[col].dtype) for col in t.columns})

    if isinstance(t.index, pd.MultiIndex):
        t.index = t.index.set_levels([level.astype(_safe_dtype(level.dtype)) for level in t.index.levels])
    else:
        t.index = t.index.astype(_safe_dtype(t.index.dtype))

    return t

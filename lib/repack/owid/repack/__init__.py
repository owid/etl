import datetime as dt
from typing import Any, Dict, List, Optional, cast

import numpy as np
import pandas as pd
import pyarrow
import pyarrow.lib


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
    if s.dtype.name.replace("[pyarrow]", "") in ("Int64", "int64", "UInt64", "uint64"):
        return shrink_integer(s.astype("int64[pyarrow]"))

    if s.dtype.name.replace("[pyarrow]", "") in ("object", "string", "float64", "Float64"):
        for strategy in [to_int, to_float, to_category]:
            try:
                return strategy(s)
            except (ValueError, TypeError, OverflowError):
                continue

    return s


def to_int(s: pd.Series) -> pd.Series:
    # values could be integers or strings
    s = _to_float64(s)
    v = s.astype("int64[pyarrow]")

    # casting to float converts strings to floats, that doesn't work with float64[pyarrow]
    if not series_eq(v, s):
        raise ValueError()

    # it's an integer, now pack it smaller
    return shrink_integer(v)


def shrink_integer(s: pd.Series) -> pd.Series:
    """
    Take an int64[pyarrow] series and make it as small as possible.
    """
    assert s.dtype.name.replace("[pyarrow]", "") in ("Int64", "int64", "UInt64", "uint64"), s.dtype

    if s.isnull().all():
        # shrink all NaNs to Int8
        return s.astype("int8[pyarrow]")
    else:
        if s.min() < 0:
            series = ["int32[pyarrow]", "int16[pyarrow]", "int8[pyarrow]"]
        else:
            series = ["uint32[pyarrow]", "uint16[pyarrow]", "uint8[pyarrow]"]

    for dtype in series:
        try:
            v = s.astype(dtype)
        except pyarrow.lib.ArrowInvalid:
            break

        if not (v == s).all():
            break

        s = v

    return s


def _to_float64(s: pd.Series) -> pd.Series:
    """Convert pandas series to float if possible. It handles object types and string types as well."""
    # Handle pyarrow types separately
    if "[pyarrow]" in s.dtype.name:
        # Directly convert to float64[pyarrow]
        return s.astype("float64[pyarrow]")
    else:
        # Convert object types to float first and then to float64[pyarrow]
        return s.replace({pd.NA: np.nan}).astype(float).astype("float64[pyarrow]")


def to_float(s: pd.Series) -> pd.Series:
    return shrink_float(_to_float64(s))


def shrink_float(s: pd.Series) -> pd.Series:
    """
    Take a float64[pyarrow] series and make it as small as possible.
    """
    assert s.dtype.name.replace("[pyarrow]", "") in ("float64", "Float64", "double"), s.dtype

    options = ["float32[pyarrow]", "float64[pyarrow]"]
    for dtype in options:
        try:
            v = s.astype(dtype)
        except pyarrow.lib.ArrowInvalid:
            continue

        if series_eq(s, v):
            return v

    raise ValueError()


def to_category(s: pd.Series) -> pd.Series:
    types = set(s.dropna().apply(type).unique())

    if types.difference({str, np.str_, dt.datetime, dt.date, type(None)}):
        raise ValueError()

    return s.astype("category")


# def is_numeric_series(s: pd.Series) -> bool:
#     """
#     Check if a pandas Series has a numeric dtype, including pyarrow numeric types.
#     """
#     if pd.api.types.is_numeric_dtype(s.dtype):
#         return True
#     elif isinstance(s.dtype, pd.ArrowDtype):
#         pa_dtype = s.dtype.pyarrow_dtype
#         return pa.types.is_integer(pa_dtype) or pa.types.is_floating(pa_dtype)
#     return False


# def series_eq(lhs: pd.Series, rhs: pd.Series, rtol: float = 1e-5, atol: float = 1e-8) -> bool:
#     """
#     Check that series are equal, considering NaNs as equal.
#     Works for all types, including those backed by pyarrow.
#     """
#     if len(lhs) != len(rhs):
#         return False

#     # Check if both Series have numeric dtypes
#     if is_numeric_series(lhs) and is_numeric_series(rhs):
#         # Convert to numpy arrays of float64, handling pyarrow types
#         lhs_values = lhs.to_numpy(dtype="float64", na_value=np.nan)
#         rhs_values = rhs.to_numpy(dtype="float64", na_value=np.nan)
#         return np.allclose(lhs_values, rhs_values, rtol=rtol, atol=atol, equal_nan=True)
#     else:
#         # For non-numeric types, use pandas' equality check that considers NaNs as equal
#         return lhs.equals(rhs)


# def series_eq(lhs: pd.Series, rhs: pd.Series, rtol: float = 1e-5, atol: float = 1e-8) -> bool:
#     """
#     Check that series are equal, but unlike normal floating point checks where
#     NaN != NaN, we want missing or null values to be reported as equal to each
#     other.
#     """
#     # NOTE: this could be speeded up with numpy methods or smarter comparison,
#     # but it's not bottleneck at the moment
#     if len(lhs) != len(rhs):
#         return False

#     return np.allclose(lhs, rhs, rtol=rtol, atol=atol, equal_nan=True)


def series_eq(
    lhs: pd.Series, rhs: pd.Series, cast: Optional[Any] = None, rtol: float = 1e-5, atol: float = 1e-8
) -> bool:
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
    elif cast is None:
        func = lambda s: s  # noqa: E731
    else:
        # NOTE: this would be extremely slow in practice
        func = lambda s: s.apply(cast)  # noqa: E731

    return np.allclose(func(lhs), func(rhs), rtol=rtol, atol=atol, equal_nan=True)


def _safe_dtype(dtype: Any) -> str:
    """Determine the appropriate dtype string based on pandas dtype."""
    if pd.api.types.is_integer_dtype(dtype):
        return "int64[pyarrow]"
    elif pd.api.types.is_float_dtype(dtype):
        return "float64[pyarrow]"
    elif isinstance(dtype, pd.CategoricalDtype):
        return "string[pyarrow]"
    elif pd.api.types.is_bool_dtype(dtype):
        return "bool[pyarrow]"
    elif dtype == object:
        return "string[pyarrow]"
    else:
        return dtype


def to_safe_types(t: pd.DataFrame) -> pd.DataFrame:
    """Convert numeric columns to float64[pyarrow] and int64[pyarrow] and categorical
    columns to string[pyarrow]."""
    t = t.astype({col: _safe_dtype(t[col].dtype) for col in t.columns})

    if isinstance(t.index, pd.MultiIndex):
        t.index = t.index.set_levels([level.astype(_safe_dtype(level.dtype)) for level in t.index.levels])
    else:
        t.index = t.index.astype(_safe_dtype(t.index.dtype))

    return t

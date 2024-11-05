import datetime as dt
from typing import Any

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from owid import repack


def test_repack_non_object_columns():
    df = pd.DataFrame({"myint": [1, 2, 3], "myfloat": [1.0, 2.2, 3.0], "mycat": ["a", "b", "c"]})
    df["mycat"] = df["mycat"].astype("category")

    df2 = df.copy()
    df2 = repack.repack_frame(df2, {})

    assert df2.myint.dtype.name == "uint8"
    assert df2.myfloat.dtype.name == "float32"
    assert_frame_equal(df, df2, check_dtype=False)


def test_repack_object_columns():
    df = pd.DataFrame(
        {
            "myint": [1, 2, None, 3],
            "myfloat": [1.2, 2.0, 3.0, None],
            "mycat": ["a", None, "b", "c"],
        },
        dtype="object",
    )

    df_repack = df.copy()

    df_repack = repack.repack_frame(df_repack)
    assert df_repack.myint.dtype.name == "UInt8"
    assert df_repack.myfloat.dtype.name == "float32"
    assert df_repack.mycat.dtype.name == "category"


def test_repack_frame_with_index():
    df = pd.DataFrame(
        {
            "myint": [1, 2, None, 3],
            "myfloat": [1.2, 2.0, 3.0, None],
            "mycat": ["a", None, "b", "c"],
        },
        dtype="object",
    )
    df.set_index(["myint", "mycat"], inplace=True)

    with pytest.raises(ValueError):
        repack.repack_frame(df)


def test_repack_integer_strings():
    s = pd.Series(["1", "2", "3", None])
    v = repack.repack_series(s)
    assert v.dtype.name == "UInt8"


def test_repack_float_strings():
    s = pd.Series(["10", "22.2", "30"])
    v = repack.repack_series(s)
    assert v.dtype.name == "float32"


def test_repack_uint64():
    s = pd.Series([10, 20], dtype="uint64")
    v = repack.repack_series(s)
    assert v.dtype.name == "uint8"


def test_repack_int8_boundaries():
    s = pd.Series([0, -1])
    info: Any = np.iinfo(np.int8)

    # check the lower boundary
    s[0] = info.min
    assert repack.repack_series(s).dtype.name == "int8"
    s[0] -= 1
    assert repack.repack_series(s).dtype.name == "int16"

    # check the upper boundary
    s[0] = info.max
    assert repack.repack_series(s).dtype.name == "int8"
    s[0] += 1
    assert repack.repack_series(s).dtype.name == "int16"


def test_repack_int16_boundaries():
    s = pd.Series([0, -1])
    info: Any = np.iinfo(np.int16)

    # check the lower boundary
    s[0] = info.min
    assert repack.repack_series(s).dtype.name == "int16"
    s[0] -= 1
    assert repack.repack_series(s).dtype.name == "int32"

    # check the upper boundary
    s[0] = info.max
    assert repack.repack_series(s).dtype.name == "int16"
    s[0] += 1
    assert repack.repack_series(s).dtype.name == "int32"


def test_repack_int32_boundaries():
    s = pd.Series([0, -1])
    info: Any = np.iinfo(np.int32)

    # check the lower boundary
    s[0] = info.min
    assert repack.repack_series(s).dtype.name == "int32"
    s[0] -= 1
    assert repack.repack_series(s).dtype.name == "int64"

    # check the upper boundary
    s[0] = info.max
    assert repack.repack_series(s).dtype.name == "int32"
    s[0] += 1
    assert repack.repack_series(s).dtype.name == "int64"


def test_repack_uint_boundaries():
    s = pd.Series([0])
    # uint8
    info: Any = np.iinfo(np.uint8)
    s[0] = info.max
    assert repack.repack_series(s).dtypes.name == "uint8"

    s[0] += 1
    assert repack.repack_series(s).dtypes.name == "uint16"

    # uint16
    info2: Any = np.iinfo(np.uint16)
    s[0] = info2.max
    assert repack.repack_series(s).dtypes.name == "uint16"

    s[0] += 1
    assert repack.repack_series(s).dtypes.name == "uint32"

    # uint32
    info3: Any = np.iinfo(np.uint32)
    s[0] = info3.max
    assert repack.repack_series(s).dtypes.name == "uint32"

    # we don't bother using uint64, we just use int64
    s[0] += 1
    assert repack.repack_series(s).dtypes.name == "int64"


def test_repack_int():
    s = pd.Series([1, 2, None, 3]).astype("object")
    v = repack.repack_series(s)
    assert v.dtype == "UInt8"


def test_repack_int_no_null():
    s = pd.Series([1, 2, 3]).astype("object")
    v = repack.repack_series(s)
    assert v.dtype == "uint8"


def test_repack_float_to_int():
    s = pd.Series([1, 2, None, 3])
    assert s.dtype == "float64"
    v = repack.repack_series(s)
    assert v.dtype == "UInt8"


def test_repack_float_object_to_float32():
    s = pd.Series([1, 2, None, 3.3], dtype="object")

    v = repack.repack_series(s)
    assert v.dtype == "float32"


def test_repack_category():
    s = pd.Series(["a", "b", "c", None])
    assert s.dtype == np.object_

    v = repack.repack_series(s)
    assert v.dtype == "category"


def test_shrink_integers_uint8():
    s = pd.Series([1, 2, 3], dtype="Int64")
    v = repack.shrink_integer(s)
    assert v.dtype.name == "uint8"


def test_shrink_integers_int8():
    s = pd.Series([1, 2, 3, -3], dtype="Int64")
    v = repack.shrink_integer(s)
    assert v.dtype.name == "int8"


def test_repack_frame_keep_dtypes():
    df = pd.DataFrame({"myint": [1, 2, 3], "myfloat": [1.0, 2.2, 3.0]})

    df2 = df.copy()
    df2 = repack.repack_frame(df2, dtypes={"myint": float})

    assert df2.myint.dtype.name == "float64"
    assert df2.myfloat.dtype.name == "float32"


def test_repack_int64_all_nans():
    s = pd.Series([np.nan, np.nan, np.nan], dtype="Int64")
    v = repack.repack_series(s)
    assert v.dtype.name == "Int8"


def test_repack_float64_all_nans():
    s = pd.Series([np.nan, np.nan, np.nan], dtype="float64")
    v = repack.repack_series(s)
    assert v.dtype.name == "Int8"


def test_series_eq():
    a = pd.Series([1, np.nan], dtype="float64")
    b = pd.Series([2, np.nan], dtype="float64")
    assert not repack.series_eq(a, b, cast=float)

    a = pd.Series([1, np.nan], dtype="float64")
    b = pd.Series([1, np.nan], dtype="float64")
    assert repack.series_eq(a, b, cast=float)


def test_repack_object_np_str():
    s = pd.Series(["a", np.str_("b")], dtype=object)
    v = repack.repack_series(s)
    assert v.dtype.name == "category"


def test_repack_with_inf():
    s = pd.Series([0, np.inf], dtype=object)
    v = repack.repack_series(s)
    assert v.dtype.name == "float32"


def test_repack_with_datetime():
    s = pd.Series([dt.datetime.today(), dt.date.today()], dtype=object)
    v = repack.repack_series(s)
    assert v.dtype.name == "category"


def test_repack_string_type():
    s = pd.Series(["a", "b", "c"]).astype("string")
    assert s.dtype == "string"

    v = repack.repack_series(s)
    assert v.dtype == "category"


def test_to_safe_types():
    # Create a DataFrame with various dtypes
    df = pd.DataFrame(
        {
            "int_col": [1, 2, 3],
            "float_col": [1.1, 2.2, 3.3],
            "cat_col": pd.Categorical(["a", "b", "c"]),
            "object_col": ["x", "y", "z"],
        }
    )

    # Set an index with integer dtype
    df.set_index("int_col", inplace=True)

    # Apply the to_safe_types function
    df_safe = repack.to_safe_types(df)

    # Check that the dtypes have been converted appropriately
    assert df_safe.index.dtype == "Int64"
    assert df_safe["float_col"].dtype == "Float64"
    assert df_safe["cat_col"].dtype == "string[python]"
    # 'object_col' should remain unchanged
    assert df_safe["object_col"].dtype == "object"


def test_to_safe_types_multiindex():
    # Create a DataFrame with MultiIndex
    df = pd.DataFrame(
        {
            "int_col": [1, 2, 3],
            "cat_col": pd.Categorical(["a", "b", "c"]),
            "float_col": [1.1, 2.2, 3.3],
        }
    )
    df.set_index(["int_col", "cat_col"], inplace=True)

    # Apply the to_safe_types function
    df_safe = repack.to_safe_types(df)

    # Check index levels
    assert df_safe.index.levels[0].dtype == "Int64"  # type: ignore
    assert df_safe.index.levels[1].dtype == "string[python]"  # type: ignore
    # Check column dtype
    assert df_safe["float_col"].dtype == "Float64"


def test_to_safe_types_with_nan():
    # Create a DataFrame with NaN values
    df = pd.DataFrame(
        {
            "int_col": [1, 2, 3],
            "float_col": [1.1, np.nan, 3.3],
            "cat_col": pd.Categorical(["a", None, "c"]),
        }
    )
    df.set_index("float_col", inplace=True)

    # Apply the to_safe_types function
    df_safe = repack.to_safe_types(df)

    # Check that NaN values are handled correctly
    assert df_safe.index.dtype == "Float64"
    assert df_safe["int_col"].dtype == "Int64"
    assert df_safe["cat_col"].dtype == "string[python]"

    # Ensure that the NA value in 'cat_col' remains pd.NA and not the string "NA"
    assert pd.isna(df_safe["cat_col"].iloc[1])
    assert df_safe["cat_col"].iloc[1] is pd.NA

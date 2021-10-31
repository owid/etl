#
#  test_frames.py
#


from typing import cast

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from etl import frames


def test_repack_non_object_columns():
    df = pd.DataFrame(
        {"myint": [1, 2, 3], "myfloat": [1.0, 2.2, 3.0], "mycat": ["a", "b", "c"]}
    )
    df["mycat"] = df["mycat"].astype("category")

    df2 = df.copy()
    frames.repack_frame(df2, {})

    assert df2.myint.dtype.name == "uint8"
    assert df2.myfloat.dtype.name == "float64"
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

    frames.repack_frame(df_repack, {})

    for col in df_repack.columns:
        assert (df_repack[col].isnull() == df[col].isnull()).all()
        assert (df_repack[col].dropna() == df[col].dropna()).all()


def test_repack_integer_strings():
    s = pd.Series(["1", "2", "3", None])
    v = frames.repack_series(s)
    assert v.dtype.name == "UInt8"


def test_repack_float_strings():
    s = pd.Series(["10", "22.2", "30"])
    v = frames.repack_series(s)
    assert v.dtype.name == "float64"


def test_repack_int8_boundaries():
    s = pd.Series([0, -1])
    info = np.iinfo(np.int8)

    # check the lower boundary
    s[0] = info.min
    assert frames.repack_series(s).dtype.name == "int8"
    s[0] -= 1
    assert frames.repack_series(s).dtype.name == "int16"

    # check the upper boundary
    s[0] = info.max
    assert frames.repack_series(s).dtype.name == "int8"
    s[0] += 1
    assert frames.repack_series(s).dtype.name == "int16"


def test_repack_int16_boundaries():
    s = pd.Series([0, -1])
    info = np.iinfo(np.int16)

    # check the lower boundary
    s[0] = info.min
    assert frames.repack_series(s).dtype.name == "int16"
    s[0] -= 1
    assert frames.repack_series(s).dtype.name == "int32"

    # check the upper boundary
    s[0] = info.max
    assert frames.repack_series(s).dtype.name == "int16"
    s[0] += 1
    assert frames.repack_series(s).dtype.name == "int32"


def test_repack_int32_boundaries():
    s = pd.Series([0, -1])
    info = np.iinfo(np.int32)

    # check the lower boundary
    s[0] = info.min
    assert frames.repack_series(s).dtype.name == "int32"
    s[0] -= 1
    assert frames.repack_series(s).dtype.name == "int64"

    # check the upper boundary
    s[0] = info.max
    assert frames.repack_series(s).dtype.name == "int32"
    s[0] += 1
    assert frames.repack_series(s).dtype.name == "int64"


def test_repack_uint_boundaries():
    s = pd.Series([0])
    # uint8
    info = np.iinfo(np.uint8)
    s[0] = info.max
    assert frames.repack_series(s).dtypes.name == "uint8"

    s[0] += 1
    assert frames.repack_series(s).dtypes.name == "uint16"

    # uint16
    info = np.iinfo(np.uint16)
    s[0] = info.max
    assert frames.repack_series(s).dtypes.name == "uint16"

    s[0] += 1
    assert frames.repack_series(s).dtypes.name == "uint32"

    # uint32
    info = np.iinfo(np.uint32)
    s[0] = info.max
    assert frames.repack_series(s).dtypes.name == "uint32"

    # we don't bother using uint64, we just use int64
    s[0] += 1
    assert frames.repack_series(s).dtypes.name == "int64"


def test_repack_int():
    s = cast(pd.Series, pd.Series([1, 2, None, 3]).astype("object"))
    v = frames.repack_series(s)
    assert v.dtype == "UInt8"


def test_repack_int_no_null():
    s = cast(pd.Series, pd.Series([1, 2, 3]).astype("object"))
    v = frames.repack_series(s)
    assert v.dtype == "uint8"


def test_repack_float_to_int():
    s = pd.Series([1, 2, None, 3])
    assert s.dtype == "float64"
    v = frames.repack_series(s)
    assert v.dtype == "UInt8"


def test_repack_float_object_to_float64():
    s = pd.Series([1, 2, None, 3.3], dtype="object")

    v = frames.repack_series(s)
    assert v.dtype == "float64"


def test_repack_category():
    s = pd.Series(["a", "b", "c", None])
    assert s.dtype == np.object_

    v = frames.repack_series(s)
    assert v.dtype == "category"


def test_shrink_integers_uint8():
    s = pd.Series([1, 2, 3], dtype="Int64")
    v = frames.shrink_integer(s)
    assert v.dtype.name == "uint8"


def test_shrink_integers_int8():
    s = pd.Series([1, 2, 3, -3], dtype="Int64")
    v = frames.shrink_integer(s)
    assert v.dtype.name == "int8"

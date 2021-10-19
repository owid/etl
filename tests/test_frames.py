#
#  test_frames.py
#


from typing import cast

import numpy as np
import pandas as pd

from etl import frames


def test_repack_non_object_columns():
    df = pd.DataFrame(
        {"myint": [1, 2, 3], "myfloat": [1.0, 2.2, 3.0], "mycat": ["a", "b", "c"]}
    )
    df["mycat"] = df["mycat"].astype("category")

    df2 = df.copy()
    frames.repack_frame(df2)

    for col in df.columns:
        assert df2[col].dtype == df[col].dtype, col

    assert (df2.dtypes == df.dtypes).all()


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

    frames.repack_frame(df_repack)

    for col in df_repack.columns:
        assert (df_repack[col].isnull() == df[col].isnull()).all()
        assert (df_repack[col].dropna() == df[col].dropna()).all()


def test_repack_int():
    s = cast(pd.Series, pd.Series([1, 2, None, 3]).astype("object"))
    v = frames.repack_series(s)
    assert v.dtype == "Int64"


def test_repack_int_no_null():
    s = cast(pd.Series, pd.Series([1, 2, 3]).astype("object"))
    v = frames.repack_series(s)
    assert v.dtype == "Int64"


def test_repack_float_to_int():
    s = pd.Series([1, 2, None, 3])
    assert s.dtype == "float64"
    v = frames.repack_series(s)
    assert v.dtype == "Int64"


def test_repack_float():
    s = pd.Series([1, 2, None, 3.3])
    assert s.dtype == "float64"
    s = cast(pd.Series, s.astype("object"))

    v = frames.repack_series(s)
    assert v.dtype == "float64"


def test_repack_category():
    s = pd.Series(["a", "b", "c", None])
    assert s.dtype == np.object_

    v = frames.repack_series(s)
    assert v.dtype == "category"

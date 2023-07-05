import numpy as np
import pandas as pd
import pytest

from etl.tempcompare import df_equals


def test_df_equals():
    # Test with two identical dataframes
    df1 = pd.DataFrame({"col1": [1, 2, 3, np.nan], "col2": ["a", "b", "c", np.nan]})
    df2 = pd.DataFrame({"col1": [1, 2, 3, np.nan], "col2": ["a", "b", "c", np.nan]})
    assert df_equals(df1, df2).all().all()

    # Test with two different dataframes
    df1 = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    df2 = pd.DataFrame({"col1": [1, 2, 3], "col2": ["d", "e", "f"]})
    assert not df_equals(df1, df2).all().all()

    # Test with numeric differences within tolerance
    df1 = pd.DataFrame({"col1": [1.0, 2.0, 3.0]})
    df2 = pd.DataFrame({"col1": [1.0, 2.00001, 3.0]})
    assert df_equals(df1, df2, absolute_tolerance=1e-05).all().all()

    # Test with categorical columns
    df1 = pd.DataFrame({"col1": pd.Categorical(["a", "b", "c"])})
    df2 = pd.DataFrame({"col1": pd.Categorical(["a", "b", "c"])})
    assert df_equals(df1, df2).all().all()


def test_df_equals_differing():
    # Test with numeric differences exceeding absolute tolerance
    df1 = pd.DataFrame({"col1": [1.0, 2.0, 3.0]})
    df2 = pd.DataFrame({"col1": [1.0, 2.0001, 3.0]})
    assert not df_equals(df1, df2, absolute_tolerance=1e-05).all().all()

    # Test with categorical columns that are not identical
    df1 = pd.DataFrame({"col1": pd.Categorical(["a", "b", "c"])})
    df2 = pd.DataFrame({"col1": pd.Categorical(["a", "b", "d"])})
    assert not df_equals(df1, df2).all().all()

    # Test with one column having NaN values
    df1 = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    df2 = pd.DataFrame({"col1": [1, 2, np.nan], "col2": ["a", "b", "c"]})
    assert not df_equals(df1, df2).all().all()

    # Test with different datatypes for the same values
    df1 = pd.DataFrame({"col1": [1, 2, 3]})
    df2 = pd.DataFrame({"col1": ["1", "2", "3"]})
    assert not df_equals(df1, df2).all().all()

    # Test with different datetimes
    df1 = pd.DataFrame({"col1": pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01"])})
    df2 = pd.DataFrame({"col1": pd.to_datetime(["2020-01-01", "2020-02-01", "2020-04-01"])})
    assert not df_equals(df1, df2).all().all()


def test_df_equals_exceptions():
    # Test exception when columns are not the same
    df1 = pd.DataFrame({"col1": [1, 2, 3]})
    df2 = pd.DataFrame({"col2": [1, 2, 3]})
    with pytest.raises(AssertionError):
        df_equals(df1, df2)

    # Test exception when indices are not the same
    df1 = pd.DataFrame({"col1": [1, 2, 3]}, index=[1, 2, 3])
    df2 = pd.DataFrame({"col1": [1, 2, 3]}, index=[4, 5, 6])
    with pytest.raises(AssertionError):
        df_equals(df1, df2)

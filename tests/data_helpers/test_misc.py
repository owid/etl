"""Test functions in etl.data_helpers.misc module."""

import numpy as np
import pandas as pd
from owid.catalog import Table

from etl.data_helpers.misc import (
    expand_time_column,
    round_to_nearest_power_of_ten,
    round_to_shifted_power_of_ten,
    round_to_sig_figs,
)


def test_round_to_sig_figs_1_sig_fig():
    tests = {
        0.01: 0.01,
        0.059: 0.06,
        0.055: 0.06,
        0.050: 0.05,
        0.0441: 0.04,
        0: 0,
        1: 1,
        5: 5,
        9: 9,
        10: 10,
        11: 10,
        15: 20,
        440.0321: 400,
        450.0321: 500,
        987: 1000,
    }
    for test in tests.items():
        assert round_to_sig_figs(test[0], sig_figs=1) == float(test[1])
        # Check also the same numbers but negative.
        assert round_to_sig_figs(-test[0], sig_figs=1) == -float(test[1])


def test_round_to_sig_figs_2_sig_fig():
    tests = {
        0.01: 0.010,
        0.059: 0.059,
        0.055: 0.055,
        0.050: 0.050,
        0.0441: 0.044,
        0: 0.0,
        1: 1.0,
        5: 5.0,
        9: 9.0,
        10: 10,
        11: 11,
        15: 15,
        440.0321: 440,
        450.0321: 450,
        987: 990,
    }
    for test in tests.items():
        assert round_to_sig_figs(test[0], sig_figs=2) == test[1]
        # Check also the same numbers but negative.
        assert round_to_sig_figs(-test[0], sig_figs=2) == -test[1]


def test_round_to_nearest_power_of_ten_floor():
    tests = {
        -0.1: -0.1,
        -0.12: -0.1,
        -90: -10,
        0: 0,
        1: 1,
        123: 100,
        1001: 1000,
        9000: 1000,
        0.87: 0.1,
        0.032: 0.01,
        0.0005: 0.0001,
    }
    for test in tests.items():
        assert round_to_nearest_power_of_ten(test[0]) == test[1], test


def test_round_to_nearest_power_of_ten_ceil():
    tests = {
        -0.1: -0.1,
        -0.12: -1,
        -90: -100,
        0: 0,
        1: 1,
        123: 1000,
        1001: 10000,
        9000: 10000,
        0.87: 1,
        0.032: 0.1,
        0.0005: 0.001,
    }
    for test in tests.items():
        assert round_to_nearest_power_of_ten(test[0], floor=False) == test[1], test


def test_round_to_shifted_power_of_ten_floor():
    tests = {
        -0.1: -0.1,
        -0.12: -0.1,
        -0.21: -0.2,
        -90: -50,
        0: 0,
        1: 1,
        -1: -1,
        123: 100,
        1001: 1000,
        3001: 3000,
        9000: 5000,
        0.87: 0.5,
        0.032: 0.03,
        0.0005: 0.0005,
    }
    for test in tests.items():
        assert round_to_shifted_power_of_ten(test[0], shifts=[1, 2, 3, 5]) == test[1], test


def test_round_to_shifted_power_of_ten_ceil():
    tests = {
        -0.1: -0.1,
        -0.12: -0.2,
        -0.21: -0.3,
        -90: -100,
        0: 0,
        1: 1,
        -1: -1,
        123: 200,
        1001: 2000,
        3001: 5000,
        9000: 10000,
        0.87: 1,
        0.032: 0.05,
        0.0005: 0.0005,
    }
    for test in tests.items():
        assert round_to_shifted_power_of_ten(test[0], shifts=[1, 2, 3, 5], floor=False) == test[1], test


def test_expand_time_column_full_range_dimension():
    data = {
        "country": ["spain", "spain", "spain", "spain", "spain", "spain", "italy", "italy", "italy"],
        "dimension": ["female", "female", "female", "male", "male", "male", "female", "female", "female"],
        "year": [2001, 2003, 2004, 2001, 2003, 2004, 2000, 2001, 2002],
        "value1": [10, 15, 20, 11, 14, 22, 5, 7, 10],
        "value2": [100, 150, 200, 111, 135, 200, 50, 70, 100],
    }
    df = pd.DataFrame(data)
    dimension_col = ["country", "dimension"]
    time_col = "year"
    index_col = dimension_col + [time_col]

    # 1/ Check complete period within dimension
    dfx = expand_time_column(
        df,
        dimension_col=["country", "dimension"],
        time_col=time_col,
        method="full_range_entity",
    )

    # Assert
    assert (
        df.groupby(dimension_col)[time_col]
        .agg(["min", "max"])
        .equals(dfx.groupby(dimension_col)[time_col].agg(["min", "max"]))
    ), "Min max years for some entities may have changed!"
    assert (
        dfx.groupby(dimension_col)[time_col].diff().dropna() == 1
    ).all(), "Time difference is sometimes greater than one unit"
    assert df.sort_values(index_col, ignore_index=True).equals(
        dfx.dropna().sort_values(index_col, ignore_index=True).astype(df.dtypes)
    ), "Original values should have been preserved"

    # Check complete period

    # Check period of observed dates for all dims
    df.set_index(["country", "year"], inplace=True)


def test_expand_time_column_full_range():
    data = {
        "country": ["spain", "spain", "spain", "spain", "spain", "spain", "italy", "italy", "italy"],
        "dimension": ["female", "female", "female", "male", "male", "male", "female", "female", "female"],
        "year": [2001, 2003, 2004, 2001, 2003, 2004, 2000, 2001, 2002],
        "value1": [10, 15, 20, 11, 14, 22, 5, 7, 10],
        "value2": [100, 150, 200, 111, 135, 200, 50, 70, 100],
    }
    df = pd.DataFrame(data)
    dimension_col = ["country", "dimension"]
    time_col = "year"
    index_col = dimension_col + [time_col]

    # 1/ Check complete period within dimension
    dfx = expand_time_column(
        df,
        dimension_col=["country", "dimension"],
        time_col=time_col,
        method="full_range",
    )

    # Assertions
    n = 1
    for col in index_col:
        n *= df[col].nunique()
    assert len(dfx) == n, "Number of combinations is not complete"
    _ = df.set_index(index_col, verify_integrity=True)

    time_range = dfx.groupby(dimension_col)[time_col].agg(["min", "max"])
    assert (time_range["min"] == df[time_col].min()).all(), "Minimum time is not as expected!"
    assert (time_range["max"] == df[time_col].max()).all(), "Maximum time is not as expected!"
    assert (
        dfx.groupby(dimension_col)[time_col].diff().dropna() == 1
    ).all(), "Time difference is sometimes greater than one unit"
    assert df.sort_values(index_col, ignore_index=True).equals(
        dfx.dropna().sort_values(index_col, ignore_index=True).astype(df.dtypes)
    ), "Original values should have been preserved"


def test_expand_time_column_observed():
    data = {
        "country": ["spain", "spain", "spain", "spain", "spain", "spain", "italy", "italy", "italy"],
        "dimension": ["female", "female", "female", "male", "male", "male", "female", "female", "female"],
        "year": [2001, 2003, 2005, 2001, 2003, 2005, 2000, 2001, 2002],
        "value1": [10, 15, 20, 11, 14, 22, 5, 7, 10],
        "value2": [100, 150, 200, 111, 135, 200, 50, 70, 100],
    }
    df = pd.DataFrame(data)
    dimension_col = ["country", "dimension"]
    time_col = "year"
    index_col = dimension_col + [time_col]

    # 1/ Check complete period within dimension
    dfx = expand_time_column(
        df,
        dimension_col=["country", "dimension"],
        time_col=time_col,
        method="observed",
    )

    # Assertions
    n = 1
    for col in index_col:
        n *= df[col].nunique()
    assert len(dfx) == n, "Number of combinations is not complete"
    _ = df.set_index(index_col, verify_integrity=True)

    times = dfx.groupby(dimension_col)[time_col].unique().apply(set)
    times_expected = set(df[time_col].unique())
    assert times.eq(times_expected).all(), "Some dimensions have unexpected times!"
    assert df.sort_values(index_col, ignore_index=True).equals(
        dfx.dropna().sort_values(index_col, ignore_index=True).astype(df.dtypes)
    ), "Original values should have been preserved"


def test_expand_time_column_none():
    data = {
        "country": ["spain", "spain", "spain", "spain", "spain", "spain", "italy", "italy", "italy"],
        "dimension": ["female", "female", "female", "male", "male", "male", "female", "female", "female"],
        "year": [2001, 2003, 2005, 2001, 2003, 2005, 2000, 2001, 2002],
        "value1": [10, 15, 20, 11, 14, 22, 5, 7, 10],
        "value2": [100, 150, 200, 111, 135, 200, 50, 70, 100],
    }
    df = pd.DataFrame(data)
    dimension_col = ["country", "dimension"]
    time_col = "year"

    # 1/ Check complete period within dimension
    dfx = expand_time_column(
        df,
        dimension_col=dimension_col,
        time_col=time_col,
        method="none",
    )

    df = df.sort_values(dimension_col).reset_index(drop=True)
    dfx = dfx.sort_values(dimension_col).reset_index(drop=True)
    assert df.equals(dfx), "Input and output dataframes were assumed to be equal"


def test_expand_time_column_fillna_basic():
    data = {
        "country": ["spain", "spain", "spain", "spain", "spain", "spain", "italy", "italy", "italy"],
        "dimension": ["female", "female", "female", "male", "male", "male", "female", "female", "female"],
        "year": [2001, 2003, 2005, 2001, 2003, 2005, 2000, 2001, 2002],
        "value1": [10, 15, 20, 11, 14, 22, 5, 7, 10],
        "value2": [100, 150, 200, 111, 135, 200, 50, 70, 100],
    }
    df = pd.DataFrame(data)
    dimension_col = ["country", "dimension"]
    time_col = "year"
    index_col = dimension_col + [time_col]

    # Check complete period within dimension
    fillna_methods = ["zero", "interpolate", "ffill", "bfill"]

    for method in fillna_methods:
        dfx = expand_time_column(
            df,
            dimension_col=["country", "dimension"],
            time_col=time_col,
            method="observed",
            fillna_method=method,
        )

        # Tag those filled
        df_tag = df[index_col]
        df_tag.loc[:, "expand"] = False

        dfx = dfx.merge(df_tag, on=index_col, how="left")
        dfx["expand"] = dfx["expand"].fillna(True).astype(bool)

        # Basic Assertions
        n = 1
        for col in index_col:
            n *= df[col].nunique()
        assert len(dfx) == n, "Number of combinations is not complete"
        _ = df.set_index(index_col, verify_integrity=True)

        times = dfx.groupby(dimension_col)[time_col].unique().apply(set)
        times_expected = set(df[time_col].unique())
        assert times.eq(times_expected).all(), "Some dimensions have unexpected times!"

        assert df.sort_values(index_col, ignore_index=True).equals(
            dfx.loc[~dfx["expand"]].drop(columns="expand").sort_values(index_col, ignore_index=True).astype(df.dtypes)
        ), "Original values should have been preserved"


def test_expand_time_column_fillna_zero():
    def _add_expand_tag(dfx, df):
        df_tag = df[index_col]
        df_tag.loc[:, "expand"] = False

        dfx = dfx.merge(df_tag, on=index_col, how="left")
        dfx["expand"] = dfx["expand"].fillna(True).astype(bool)
        return dfx

    data = {
        "country": ["spain", "spain", "spain", "spain", "spain", "spain", "italy", "italy", "italy"],
        "dimension": ["female", "female", "female", "male", "male", "male", "female", "female", "female"],
        "year": [2001, 2003, 2005, 2001, 2003, 2005, 2000, 2001, 2002],
        "value1": [10, 15, 20, 11, 14, 22, 5, 7, 10],
        "value2": [100, 150, 200, 111, 135, 200, 50, 70, 100],
    }
    df = pd.DataFrame(data)
    dimension_col = ["country", "dimension"]
    time_col = "year"
    index_col = dimension_col + [time_col]
    values_col = ["value1", "value2"]

    # Zero-filling
    dfx = expand_time_column(
        df,
        dimension_col=["country", "dimension"],
        time_col=time_col,
        method="observed",
        fillna_method="zero",
    )
    dfx = _add_expand_tag(dfx, df)

    assert (dfx.loc[dfx["expand"], values_col] == 0).all().all(), "All filled values should be zero!"


def test_expand_time_column_fillna_ffill():
    def _add_expand_tag(dfx, df):
        df_tag = df[index_col]
        df_tag.loc[:, "expand"] = False

        dfx = dfx.merge(df_tag, on=index_col, how="left")
        dfx["expand"] = dfx["expand"].fillna(True).astype(bool)
        return dfx

    data = {
        "country": ["spain", "spain", "spain", "spain", "spain", "spain", "italy", "italy", "italy"],
        "dimension": ["female", "female", "female", "male", "male", "male", "female", "female", "female"],
        "year": [2001, 2003, 2005, 2001, 2003, 2005, 2000, 2001, 2002],
        "value1": [10, 15, 20, 11, 14, 22, 5, 7, 10],
        "value2": [100, 150, 200, 111, 135, 200, 50, 70, 100],
    }
    df = pd.DataFrame(data)
    dimension_col = ["country", "dimension"]
    time_col = "year"
    index_col = dimension_col + [time_col]

    # Forward-filling
    dfx = expand_time_column(
        df,
        dimension_col=["country", "dimension"],
        time_col=time_col,
        method="observed",
        fillna_method="ffill",
    )
    dfx = _add_expand_tag(dfx, df)

    value1_expected = [
        5.0,
        7.0,
        10.0,
        10.0,
        10.0,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        10.0,
        10.0,
        15.0,
        20.0,
        np.nan,
        11.0,
        11.0,
        14.0,
        22.0,
    ]
    value2_expected = [
        50.0,
        70.0,
        100.0,
        100.0,
        100.0,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        100.0,
        100.0,
        150.0,
        200.0,
        np.nan,
        111.0,
        111.0,
        135.0,
        200.0,
    ]
    assert dfx["value1"].equals(pd.Series(value1_expected)), "Unexpected timeseries!"
    assert dfx["value2"].equals(pd.Series(value2_expected)), "Unexpected timeseries!"


def test_expand_time_column_fillna_bfill():
    def _add_expand_tag(dfx, df):
        df_tag = df[index_col]
        df_tag.loc[:, "expand"] = False

        dfx = dfx.merge(df_tag, on=index_col, how="left")
        dfx["expand"] = dfx["expand"].fillna(True).astype(bool)
        return dfx

    data = {
        "country": ["spain", "spain", "spain", "spain", "spain", "spain", "italy", "italy", "italy"],
        "dimension": ["female", "female", "female", "male", "male", "male", "female", "female", "female"],
        "year": [2001, 2003, 2005, 2001, 2003, 2005, 2000, 2001, 2002],
        "value1": [10, 15, 20, 11, 14, 22, 5, 7, 10],
        "value2": [100, 150, 200, 111, 135, 200, 50, 70, 100],
    }
    df = pd.DataFrame(data)
    dimension_col = ["country", "dimension"]
    time_col = "year"
    index_col = dimension_col + [time_col]

    # Forward-filling
    dfx = expand_time_column(
        df,
        dimension_col=["country", "dimension"],
        time_col=time_col,
        method="observed",
        fillna_method="bfill",
    )
    dfx = _add_expand_tag(dfx, df)

    value1_expected = [
        5.0,
        7.0,
        10.0,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        10.0,
        10.0,
        15.0,
        15.0,
        20.0,
        11.0,
        11.0,
        14.0,
        14.0,
        22.0,
    ]
    value2_expected = [
        50.0,
        70.0,
        100.0,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        100.0,
        100.0,
        150.0,
        150.0,
        200.0,
        111.0,
        111.0,
        135.0,
        135.0,
        200.0,
    ]
    assert dfx["value1"].equals(pd.Series(value1_expected)), "Unexpected timeseries!"
    assert dfx["value2"].equals(pd.Series(value2_expected)), "Unexpected timeseries!"


def test_expand_time_column_fillna_interpolate():
    def _add_expand_tag(dfx, df):
        df_tag = df[index_col]
        df_tag.loc[:, "expand"] = False

        dfx = dfx.merge(df_tag, on=index_col, how="left")
        dfx["expand"] = dfx["expand"].fillna(True).astype(bool)
        return dfx

    data = {
        "country": ["spain", "spain", "spain", "spain", "spain", "spain", "italy", "italy", "italy"],
        "dimension": ["female", "female", "female", "male", "male", "male", "female", "female", "female"],
        "year": [2001, 2003, 2005, 2001, 2003, 2005, 2000, 2001, 2002],
        "value1": [10, 15, 20, 11, 14, 22, 5, 7, 10],
        "value2": [100, 150, 200, 111, 135, 200, 50, 70, 100],
    }
    df = pd.DataFrame(data)
    dimension_col = ["country", "dimension"]
    time_col = "year"
    index_col = dimension_col + [time_col]

    # Forward-filling
    dfx = expand_time_column(
        df,
        dimension_col=["country", "dimension"],
        time_col=time_col,
        method="observed",
        fillna_method="interpolate",
    )
    dfx = _add_expand_tag(dfx, df)

    value1_expected = [
        5.0,
        7.0,
        10.0,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        10.0,
        12.5,
        15.0,
        20.0,
        np.nan,
        11.0,
        12.5,
        14.0,
        22.0,
    ]
    value2_expected = [
        50.0,
        70.0,
        100.0,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        100.0,
        125.0,
        150.0,
        200.0,
        np.nan,
        111.0,
        123.0,
        135.0,
        200.0,
    ]
    assert dfx["value1"].equals(pd.Series(value1_expected)), "Unexpected timeseries!"
    assert dfx["value2"].equals(pd.Series(value2_expected)), "Unexpected timeseries!"


def test_expand_time_column_fillna_interpolate_and_zero():
    def _add_expand_tag(dfx, df):
        df_tag = df[index_col]
        df_tag.loc[:, "expand"] = False

        dfx = dfx.merge(df_tag, on=index_col, how="left")
        dfx["expand"] = dfx["expand"].fillna(True).astype(bool)
        return dfx

    data = {
        "country": ["spain", "spain", "spain", "spain", "spain", "spain", "italy", "italy", "italy"],
        "dimension": ["female", "female", "female", "male", "male", "male", "female", "female", "female"],
        "year": [2001, 2003, 2005, 2001, 2003, 2005, 2000, 2001, 2002],
        "value1": [10, 15, 20, 11, 14, 22, 5, 7, 10],
        "value2": [100, 150, 200, 111, 135, 200, 50, 70, 100],
    }
    df = pd.DataFrame(data)
    dimension_col = ["country", "dimension"]
    time_col = "year"
    index_col = dimension_col + [time_col]

    # Forward-filling
    dfx = expand_time_column(
        df,
        dimension_col=["country", "dimension"],
        time_col=time_col,
        method="observed",
        fillna_method=["interpolate", "zero"],
    )
    dfx = _add_expand_tag(dfx, df)

    value1_expected = [
        5.0,
        7.0,
        10.0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        10.0,
        12.5,
        15.0,
        20.0,
        0,
        11.0,
        12.5,
        14.0,
        22.0,
    ]
    value2_expected = [
        50.0,
        70.0,
        100.0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        100.0,
        125.0,
        150.0,
        200.0,
        0,
        111.0,
        123.0,
        135.0,
        200.0,
    ]
    assert dfx["value1"].equals(pd.Series(value1_expected)), "Unexpected timeseries!"
    assert dfx["value2"].equals(pd.Series(value2_expected)), "Unexpected timeseries!"


def test_expand_time_column_and_extra_years():
    def _add_expand_tag(dfx, df):
        df_tag = df[index_col]
        df_tag.loc[:, "expand"] = False

        dfx = dfx.merge(df_tag, on=index_col, how="left")
        dfx["expand"] = dfx["expand"].fillna(True).astype(bool)
        return dfx

    data = {
        "country": ["spain", "spain", "spain", "spain", "spain", "spain", "italy", "italy", "italy"],
        "dimension": ["female", "female", "female", "male", "male", "male", "female", "female", "female"],
        "year": [2001, 2003, 2005, 2001, 2003, 2005, 2000, 2001, 2002],
        "value1": [10, 15, 20, 11, 14, 22, 5, 7, 10],
        "value2": [100, 150, 200, 111, 135, 200, 50, 70, 100],
    }
    df = Table(data)
    dimension_col = ["country", "dimension"]
    time_col = "year"
    index_col = dimension_col + [time_col]

    # Forward-filling
    dfx = expand_time_column(
        df,
        dimension_col=["country", "dimension"],
        time_col=time_col,
        method="observed",
        since_time=1995,
        # until_time=2010,
        # fillna_method=["interpolate", "zero"],
    )
    dfx = _add_expand_tag(dfx, df)

    assert dfx.loc[dfx["expand"], ["value1", "value2"]].isna().all().all(), "All new entries should be NaNs!"


def test_expand_time_complex():
    """This example we:

    - Only extend timeseries with observed.
    - Then add years at first and at last.
    - We then fill the NaNs by first interpolating (fixes inter-values), back-filling (fixes first) and forward-filling (fixes last). We assert the equivalence interpolate-bfill-ffill = interpolate-ffill-bfill.
    """

    def _add_expand_tag(dfx, df):
        df_tag = df[index_col]
        df_tag.loc[:, "expand"] = False

        dfx = dfx.merge(df_tag, on=index_col, how="left")
        dfx["expand"] = dfx["expand"].fillna(True).astype(bool)
        return dfx

    data = {
        "country": ["spain", "spain", "spain", "spain", "spain", "spain", "italy", "italy", "italy"],
        "dimension": ["female", "female", "female", "male", "male", "male", "female", "female", "female"],
        "year": [2001, 2003, 2005, 2001, 2003, 2005, 2000, 2001, 2002],
        "value1": [10, 15, 20, 11, 14, 22, 5, 7, 10],
        "value2": [100, 150, 200, 111, 135, 200, 50, 70, 100],
    }
    df = Table(data)
    dimension_col = ["country", "dimension"]
    time_col = "year"
    index_col = dimension_col + [time_col]

    # Complex example
    dfx = expand_time_column(
        df,
        dimension_col=["country", "dimension"],
        time_col=time_col,
        method="observed",
        since_time=1995,
        until_time=2010,
        fillna_method=["interpolate", "bfill", "ffill", "zero"],
    )
    dfx = _add_expand_tag(dfx, df)

    assert dfx[["value1", "value2"]].notna().all().all(), "NaN detected!"

    value1_expected = [
        5.0,
        5.0,
        5.0,
        5.0,
        5.0,
        5.0,
        7.0,
        10.0,
        10.0,
        10.0,
        10.0,
        10.0,
        10.0,
        10.0,
        10.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        10.0,
        10.0,
        10.0,
        10.0,
        10.0,
        10.0,
        10.0,
        12.5,
        15.0,
        20.0,
        20.0,
        20.0,
        20.0,
        20.0,
        20.0,
        11.0,
        11.0,
        11.0,
        11.0,
        11.0,
        11.0,
        11.0,
        12.5,
        14.0,
        22.0,
        22.0,
        22.0,
        22.0,
        22.0,
        22.0,
    ]
    value2_expected = [
        50.0,
        50.0,
        50.0,
        50.0,
        50.0,
        50.0,
        70.0,
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        125.0,
        150.0,
        200.0,
        200.0,
        200.0,
        200.0,
        200.0,
        200.0,
        111.0,
        111.0,
        111.0,
        111.0,
        111.0,
        111.0,
        111.0,
        123.0,
        135.0,
        200.0,
        200.0,
        200.0,
        200.0,
        200.0,
        200.0,
    ]
    assert dfx["value1"].equals(pd.Series(value1_expected, name="value1")), "Unexpected timeseries!"
    assert dfx["value2"].equals(pd.Series(value2_expected)), "Unexpected timeseries!"

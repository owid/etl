"""Test functions in owid.datautils.dataframes module.

"""

import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pytest import raises, warns

from owid.datautils import dataframes


class TestCompareDataFrames:
    def test_with_large_absolute_tolerance_all_equal(self):
        assert dataframes.compare(
            df1=pd.DataFrame({"col_01": [1, 2]}),
            df2=pd.DataFrame({"col_01": [2, 3]}),
            absolute_tolerance=1,
            relative_tolerance=1e-8,
        ).equals(pd.DataFrame({"col_01": [True, True]}))

    def test_with_large_absolute_tolerance_all_unequal(self):
        assert dataframes.compare(
            df1=pd.DataFrame({"col_01": [1, 2]}),
            df2=pd.DataFrame({"col_01": [2, 3]}),
            absolute_tolerance=0.9,
            relative_tolerance=1e-8,
        ).equals(pd.DataFrame({"col_01": [False, False]}))

    def test_with_large_absolute_tolerance_mixed(self):
        assert dataframes.compare(
            df1=pd.DataFrame({"col_01": [1, 2]}),
            df2=pd.DataFrame({"col_01": [2, 3.1]}),
            absolute_tolerance=1,
            relative_tolerance=1e-8,
        ).equals(pd.DataFrame({"col_01": [True, False]}))

    def test_with_large_relative_tolerance_all_equal(self):
        assert dataframes.compare(
            df1=pd.DataFrame({"col_01": [1, 2]}),
            df2=pd.DataFrame({"col_01": [2, 3]}),
            absolute_tolerance=1e-8,
            relative_tolerance=0.5,
        ).equals(pd.DataFrame({"col_01": [True, True]}))

    def test_with_large_relative_tolerance_all_unequal(self):
        assert dataframes.compare(
            df1=pd.DataFrame({"col_01": [1, 2]}),
            df2=pd.DataFrame({"col_01": [2, 3]}),
            absolute_tolerance=1e-8,
            relative_tolerance=0.3,
        ).equals(pd.DataFrame({"col_01": [False, False]}))

    def test_with_large_relative_tolerance_mixed(self):
        assert dataframes.compare(
            df1=pd.DataFrame({"col_01": [1, 2]}),
            df2=pd.DataFrame({"col_01": [2, 3]}),
            absolute_tolerance=1e-8,
            relative_tolerance=0.4,
        ).equals(pd.DataFrame({"col_01": [False, True]}))

    def test_with_dataframes_of_equal_values_but_different_indexes(self):
        # Even if dataframes are not identical, compare_dataframes should return all Trues (since it does not care about
        # indexes, only values).
        assert dataframes.compare(
            df1=pd.DataFrame({"col_01": [1, 2], "col_02": ["a", "b"]}).set_index("col_02"),
            df2=pd.DataFrame({"col_01": [1, 2], "col_02": ["a", "c"]}).set_index("col_02"),
        ).equals(pd.DataFrame({"col_01": [True, True]}))

    def test_with_two_dataframes_with_object_columns_with_nans(self):
        assert dataframes.compare(
            df1=pd.DataFrame({"col_01": [np.nan, "b", "c"]}),
            df2=pd.DataFrame({"col_01": [np.nan, "b", "c"]}),
        ).equals(pd.DataFrame({"col_01": [True, True, True]}))


class TestAreDataFramesEqual:
    def test_on_equal_dataframes_with_one_integer_column(self):
        assert dataframes.are_equal(
            df1=pd.DataFrame({"col_01": [1, 2, 3]}),
            df2=pd.DataFrame({"col_01": [1, 2, 3]}),
        )[0]

    def test_on_almost_equal_dataframes_but_differing_by_one_element(self):
        assert not dataframes.are_equal(
            df1=pd.DataFrame({"col_01": [1, 2, 3]}),
            df2=pd.DataFrame({"col_01": [1, 2, 0]}),
        )[0]

    def test_on_almost_equal_dataframes_but_differing_by_type(self):
        assert not dataframes.are_equal(
            df1=pd.DataFrame({"col_01": [1, 2, 3]}),
            df2=pd.DataFrame({"col_01": [1, 2, 3.0]}),
        )[0]

    def test_on_equal_dataframes_containing_nans(self):
        assert dataframes.are_equal(
            df1=pd.DataFrame({"col_01": [1, 2, np.nan]}),
            df2=pd.DataFrame({"col_01": [1, 2, np.nan]}),
        )[0]

    def test_on_equal_dataframes_containing_only_nans(self):
        assert dataframes.are_equal(
            df1=pd.DataFrame({"col_01": [np.nan, np.nan]}),
            df2=pd.DataFrame({"col_01": [np.nan, np.nan]}),
        )[0]

    def test_on_equal_dataframes_both_empty(self):
        assert dataframes.are_equal(df1=pd.DataFrame(), df2=pd.DataFrame())[0]

    def test_on_equal_dataframes_with_various_types_of_columns(self):
        assert dataframes.are_equal(
            df1=pd.DataFrame(
                {
                    "col_01": [1, 2],
                    "col_02": [0.1, 0.2],
                    "col_03": ["1", "2"],
                    "col_04": [True, False],
                }
            ),
            df2=pd.DataFrame(
                {
                    "col_01": [1, 2],
                    "col_02": [0.1, 0.2],
                    "col_03": ["1", "2"],
                    "col_04": [True, False],
                }
            ),
        )[0]

    def test_on_almost_equal_dataframes_but_columns_sorted_differently(self):
        assert not dataframes.are_equal(
            df1=pd.DataFrame(
                {
                    "col_01": [1, 2],
                    "col_02": [0.1, 0.2],
                    "col_03": ["1", "2"],
                    "col_04": [True, False],
                }
            ),
            df2=pd.DataFrame(
                {
                    "col_02": [0.1, 0.2],
                    "col_01": [1, 2],
                    "col_03": ["1", "2"],
                    "col_04": [True, False],
                }
            ),
        )[0]

    def test_on_unequal_dataframes_with_all_columns_different(self):
        assert not dataframes.are_equal(
            df1=pd.DataFrame({"col_01": [1, 2], "col_02": [0.1, 0.2]}),
            df2=pd.DataFrame({"col_03": [0.1, 0.2], "col_04": [1, 2]}),
        )[0]

    def test_on_unequal_dataframes_with_some_common_columns(self):
        assert not dataframes.are_equal(
            df1=pd.DataFrame({"col_01": [1, 2], "col_02": [0.1, 0.2]}),
            df2=pd.DataFrame({"col_01": [1, 2], "col_03": [1, 2]}),
        )[0]

    def test_on_equal_dataframes_given_large_absolute_tolerance(self):
        assert dataframes.are_equal(
            df1=pd.DataFrame({"col_01": [10, 20]}),
            df2=pd.DataFrame({"col_01": [11, 21]}),
            absolute_tolerance=1,
            relative_tolerance=1e-8,
        )[0]

    def test_on_unequal_dataframes_given_large_absolute_tolerance(self):
        assert not dataframes.are_equal(
            df1=pd.DataFrame({"col_01": [10, 20]}),
            df2=pd.DataFrame({"col_01": [11, 21]}),
            absolute_tolerance=0.9,
            relative_tolerance=1e-8,
        )[0]

    def test_on_equal_dataframes_given_large_relative_tolerance(self):
        assert dataframes.are_equal(
            df1=pd.DataFrame({"col_01": [1]}),
            df2=pd.DataFrame({"col_01": [2]}),
            absolute_tolerance=1e-8,
            relative_tolerance=0.5,
        )[0]

    def test_on_unequal_dataframes_given_large_relative_tolerance(self):
        assert not dataframes.are_equal(
            df1=pd.DataFrame({"col_01": [1]}),
            df2=pd.DataFrame({"col_01": [2]}),
            absolute_tolerance=1e-8,
            relative_tolerance=0.49,
        )[0]

    def test_on_equal_dataframes_with_non_numeric_indexes(self):
        assert dataframes.are_equal(
            df1=pd.DataFrame({"col_01": [1, 2], "col_02": ["a", "b"]}).set_index("col_02"),
            df2=pd.DataFrame({"col_01": [1, 2], "col_02": ["a", "b"]}).set_index("col_02"),
        )[0]

    def test_on_dataframes_of_equal_values_but_different_indexes(self):
        assert not dataframes.are_equal(
            df1=pd.DataFrame({"col_01": [1, 2], "col_02": ["a", "b"]}).set_index("col_02"),
            df2=pd.DataFrame({"col_01": [1, 2], "col_02": ["a", "c"]}).set_index("col_02"),
        )[0]

    def test_on_dataframes_with_object_columns_with_nans(self):
        assert dataframes.are_equal(
            df1=pd.DataFrame({"col_01": [np.nan, "b", "c"]}),
            df2=pd.DataFrame({"col_01": [np.nan, "b", "c"]}),
        )[0]


class TestGroupbyAggregate:
    def test_default_aggregate_single_groupby_column_as_string(self):
        df_in = pd.DataFrame(
            {
                "year": [2001, 2003, 2003, 2003, 2002, 2002],
                "value_01": [1, 2, 3, 4, 5, 6],
            }
        )
        df_out = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "value_01": [1, 11, 9],
            }
        ).set_index("year")
        assert dataframes.groupby_agg(
            df_in,
            "year",
            aggregations=None,
            num_allowed_nans=None,
            frac_allowed_nans=None,
        ).equals(df_out)

    def test_default_aggregate_single_groupby_column_as_list(self):
        df_in = pd.DataFrame(
            {
                "year": [2001, 2003, 2003, 2003, 2002, 2002],
                "value_01": [1, 2, 3, 4, 5, 6],
            }
        )
        df_out = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "value_01": [1, 11, 9],
            }
        ).set_index("year")
        assert dataframes.groupby_agg(
            df_in,
            ["year"],
            aggregations=None,
            num_allowed_nans=None,
            frac_allowed_nans=None,
        ).equals(df_out)

    def test_default_aggregate_with_some_nans_ignored(self):
        df_in = pd.DataFrame(
            {
                "year": [2001, 2002, 2002, 2003, 2003, 2003],
                "value_01": [np.nan, 2, np.nan, 4, 5, 6],
            }
        )
        df_out = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "value_01": [0.0, 2.0, 15.0],
            }
        ).set_index("year")
        assert dataframes.groupby_agg(
            df_in,
            ["year"],
            aggregations=None,
            num_allowed_nans=None,
            frac_allowed_nans=None,
        ).equals(df_out)

    def test_default_aggregate_with_some_nans_ignored_different_types(self):
        df_in = pd.DataFrame(
            {
                "year": [2001, 2002, 2002, 2003, 2003, 2003],
                "value_01": [np.nan, 2, np.nan, 4, 5, 6],
                "value_02": ["a", "b", "c", "d", "e", "f"],
                "value_03": [True, False, False, True, True, False],
            }
        )
        df_out = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "value_01": [0.0, 2.0, 15.0],
                "value_02": ["a", "bc", "def"],
                "value_03": [1, 0, 2],
            }
        ).set_index("year")
        assert dataframes.groupby_agg(
            df_in,
            ["year"],
            aggregations=None,
            num_allowed_nans=None,
            frac_allowed_nans=None,
        ).equals(df_out)

    def test_default_aggregate_with_some_nans_ignored_different_types_and_more_nans(
        self,
    ):
        df_in = pd.DataFrame(
            {
                "year": [2001, 2002, 2002, 2003, 2003, 2003],
                "value_01": [np.nan, 2, np.nan, 4, 5, 6],
                "value_02": [np.nan, "b", np.nan, "d", "e", "f"],
                "value_03": [np.nan, False, False, True, True, np.nan],
            }
        )
        df_out = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "value_01": [0.0, 2.0, 15.0],
                "value_02": [0, "b", "def"],
                "value_03": [0, 0, 2],
            }
        ).set_index("year")
        df_out["value_03"] = df_out["value_03"].astype(object)
        assert dataframes.groupby_agg(
            df_in,
            ["year"],
            aggregations=None,
            num_allowed_nans=None,
            frac_allowed_nans=None,
        ).equals(df_out)

    def test_default_aggregate_with_num_allowed_nans_zero(self):
        df_in = pd.DataFrame(
            {
                "year": [2001, 2002, 2002, 2003, 2003, 2003],
                "value_01": [np.nan, 2, np.nan, 4, 5, 6],
                "value_02": [np.nan, "b", np.nan, "d", "e", "f"],
                "value_03": [np.nan, False, False, True, True, np.nan],
            }
        )
        df_out = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "value_01": [np.nan, np.nan, 15.0],
                "value_02": [np.nan, np.nan, "def"],
            }
        ).set_index("year")
        df_out["value_03"] = pd.Series([np.nan, 0, np.nan], index=[2001, 2002, 2003], dtype=object)
        assert dataframes.are_equal(
            df1=dataframes.groupby_agg(
                df_in,
                ["year"],
                aggregations=None,
                num_allowed_nans=0,
                frac_allowed_nans=None,
            ),
            df2=df_out,
        )[0]

    def test_default_aggregate_with_num_allowed_nans_one(self):
        df_in = pd.DataFrame(
            {
                "year": [2001, 2002, 2002, 2003, 2003, 2003],
                "value_01": [np.nan, 2, np.nan, 4, 5, 6],
                "value_02": [np.nan, "b", np.nan, "d", "e", "f"],
                "value_03": [np.nan, False, False, True, np.nan, np.nan],
            }
        )
        df_out = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "value_01": [0.0, 2.0, 15.0],
                "value_02": [0, "b", "def"],
            }
        ).set_index("year")
        df_out["value_03"] = pd.Series([0, 0, np.nan], index=[2001, 2002, 2003], dtype=object)
        assert dataframes.are_equal(
            df1=dataframes.groupby_agg(
                df_in,
                ["year"],
                aggregations=None,
                num_allowed_nans=1,
                frac_allowed_nans=None,
            ),
            df2=df_out,
        )[0]

    def test_default_aggregate_with_num_allowed_nans_two(self):
        df_in = pd.DataFrame(
            {
                "year": [2001, 2002, 2002, 2003, 2003, 2003],
                "value_01": [np.nan, 2, np.nan, 4, 5, 6],
                "value_02": [np.nan, "b", np.nan, "d", "e", "f"],
                "value_03": [np.nan, False, False, True, np.nan, np.nan],
            }
        )
        df_out = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "value_01": [0.0, 2.0, 15.0],
                "value_02": [0, "b", "def"],
            }
        ).set_index("year")
        df_out["value_03"] = pd.Series([0, 0, 1], index=[2001, 2002, 2003], dtype=object)
        assert dataframes.are_equal(
            df1=dataframes.groupby_agg(
                df_in,
                ["year"],
                aggregations=None,
                num_allowed_nans=2,
                frac_allowed_nans=None,
            ),
            df2=df_out,
        )[0]

    def test_default_aggregate_with_num_allowed_nans_the_length_of_the_dataframe(self):
        df_in = pd.DataFrame(
            {
                "year": [2001, 2002, 2002, 2004, 2004, 2004, 2004],
                "value_01": [np.nan, 2, np.nan, 4, 5, 6, 7],
                "value_02": [np.nan, "b", np.nan, "d", "e", "f", "g"],
                "value_03": [np.nan, False, False, True, np.nan, np.nan, np.nan],
            }
        )
        df_out = pd.DataFrame(
            {
                "year": [2001, 2002, 2004],
                "value_01": [0.0, 2.0, 22.0],
                "value_02": [0, "b", "defg"],
            }
        ).set_index("year")
        df_out["value_03"] = pd.Series([0, 0, 1], index=[2001, 2002, 2004], dtype=object)
        assert dataframes.are_equal(
            df1=dataframes.groupby_agg(
                df_in,
                ["year"],
                aggregations=None,
                num_allowed_nans=None,
                frac_allowed_nans=len(df_in),
            ),
            df2=df_out,
        )[0]

    def test_default_aggregate_with_frac_allowed_nans_zero(self):
        df_in = pd.DataFrame(
            {
                "year": [2001, 2002, 2002, 2003, 2003, 2003],
                "value_01": [np.nan, 2, np.nan, 4, 5, 6],
                "value_02": [np.nan, "b", np.nan, "d", "e", "f"],
                "value_03": [np.nan, False, False, True, True, np.nan],
            }
        )
        df_out = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "value_01": [np.nan, np.nan, 15.0],
                "value_02": [np.nan, np.nan, "def"],
            }
        ).set_index("year")
        df_out["value_03"] = pd.Series([np.nan, 0, np.nan], index=[2001, 2002, 2003], dtype=object)
        assert dataframes.are_equal(
            df1=dataframes.groupby_agg(
                df_in,
                ["year"],
                aggregations=None,
                num_allowed_nans=None,
                frac_allowed_nans=0,
            ),
            df2=df_out,
        )[0]

    def test_default_aggregate_with_frac_allowed_nans_half(self):
        df_in = pd.DataFrame(
            {
                "year": [2001, 2002, 2002, 2003, 2003, 2003],
                "value_01": [np.nan, 2, np.nan, 4, 5, 6],
                "value_02": [np.nan, "b", np.nan, "d", "e", "f"],
                "value_03": [np.nan, False, False, True, np.nan, np.nan],
            }
        )
        df_out = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "value_01": [np.nan, 2.0, 15.0],
                "value_02": [np.nan, "b", "def"],
            }
        ).set_index("year")
        df_out["value_03"] = pd.Series([np.nan, 0, np.nan], index=[2001, 2002, 2003], dtype=object)
        assert dataframes.are_equal(
            df1=dataframes.groupby_agg(
                df_in,
                ["year"],
                aggregations=None,
                num_allowed_nans=None,
                frac_allowed_nans=0.5,
            ),
            df2=df_out,
        )[0]

    def test_default_aggregate_with_frac_allowed_nans_two_thirds(self):
        df_in = pd.DataFrame(
            {
                "year": [2001, 2002, 2002, 2003, 2003, 2003],
                "value_01": [np.nan, 2, np.nan, 4, 5, 6],
                "value_02": [np.nan, "b", np.nan, "d", "e", "f"],
                "value_03": [np.nan, False, False, True, np.nan, np.nan],
            }
        )
        df_out = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "value_01": [np.nan, 2.0, 15.0],
                "value_02": [np.nan, "b", "def"],
            }
        ).set_index("year")
        df_out["value_03"] = pd.Series([np.nan, 0, 1], index=[2001, 2002, 2003], dtype=object)
        assert dataframes.are_equal(
            df1=dataframes.groupby_agg(
                df_in,
                ["year"],
                aggregations=None,
                num_allowed_nans=None,
                frac_allowed_nans=0.67,
            ),
            df2=df_out,
        )[0]

    def test_default_aggregate_with_frac_allowed_nans_one(self):
        df_in = pd.DataFrame(
            {
                "year": [2001, 2002, 2002, 2003, 2003, 2003, 2004, 2004, 2004, 2004],
                "value_01": [np.nan, 2, np.nan, 4, 5, 6, 7, np.nan, np.nan, np.nan],
                "value_02": [np.nan, "b", np.nan, "d", "e", "f", "g", "h", "i", "j"],
                "value_03": [
                    np.nan,
                    False,
                    False,
                    True,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    True,
                ],
            }
        )
        df_out = pd.DataFrame(
            {
                "year": [2001, 2002, 2003, 2004],
                "value_01": [0, 2.0, 15.0, 7],
                "value_02": [0, "b", "def", "ghij"],
            }
        ).set_index("year")
        df_out["value_03"] = pd.Series([0, 0, 1, 1], index=[2001, 2002, 2003, 2004], dtype=object)
        assert dataframes.are_equal(
            df1=dataframes.groupby_agg(
                df_in,
                ["year"],
                aggregations=None,
                num_allowed_nans=None,
                frac_allowed_nans=None,
            ),
            df2=df_out,
        )[0]

    def test_default_aggregate_with_both_num_allowed_nans_and_frac_allowed_nans(self):
        df_in = pd.DataFrame(
            {
                "year": [2001, 2002, 2002, 2003, 2003, 2003, 2004, 2004, 2004, 2004],
                "value_01": [np.nan, 2, np.nan, 4, 5, 6, 7, np.nan, np.nan, np.nan],
                "value_02": [np.nan, "b", np.nan, "d", "e", "f", "g", "h", "i", "j"],
                "value_03": [
                    np.nan,
                    False,
                    False,
                    True,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    True,
                ],
            }
        )
        df_out = pd.DataFrame(
            {
                "year": [2001, 2002, 2003, 2004],
                "value_01": [np.nan, 2.0, 15.0, np.nan],
                "value_02": [np.nan, "b", "def", "ghij"],
            }
        ).set_index("year")
        df_out["value_03"] = pd.Series([np.nan, 0, np.nan, np.nan], index=[2001, 2002, 2003, 2004], dtype=object)
        assert dataframes.are_equal(
            df1=dataframes.groupby_agg(
                df_in,
                ["year"],
                aggregations=None,
                num_allowed_nans=2,
                frac_allowed_nans=0.5,
            ),
            df2=df_out,
        )[0]

    def test_default_aggregate_with_two_groupby_columns(self):
        df_in = pd.DataFrame(
            {
                "country": [
                    "country_a",
                    "country_a",
                    "country_a",
                    "country_b",
                    "country_b",
                    "country_c",
                ],
                "year": [2001, 2002, 2002, 2003, 2003, 2003],
                "value_01": [1, 2, 3, 4, 5, 6],
            }
        )
        df_out = pd.DataFrame(
            {
                "country": ["country_a", "country_a", "country_b", "country_c"],
                "year": [2001, 2002, 2003, 2003],
                "value_01": [1, 5, 9, 6],
            }
        ).set_index(["country", "year"])
        assert dataframes.are_equal(
            df1=dataframes.groupby_agg(
                df_in,
                ["country", "year"],
                aggregations=None,
                num_allowed_nans=None,
                frac_allowed_nans=None,
            ),
            df2=df_out,
        )[0]

    def test_custom_aggregate(self):
        aggregations = {"value_01": "sum", "value_02": "mean"}
        df_in = pd.DataFrame(
            {
                "year": [2001, 2002, 2002, 2003, 2003, 2003],
                "value_01": [1, 2, 3, 4, 5, np.nan],
                "value_02": [1, 2, 3, 4, 5, 6],
            }
        )
        df_out = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "value_01": [1.0, 5.0, np.nan],
                "value_02": [1, 2.5, 7.5],
            }
        ).set_index("year")
        assert dataframes.are_equal(
            df1=dataframes.groupby_agg(
                df_in,
                ["year"],
                aggregations=aggregations,
                num_allowed_nans=0,
                frac_allowed_nans=None,
            ),
            df2=df_out,
        )  # type: ignore

    def test_categorical_agg(self):
        # Make sure grouped object only contains observed combinations of categories
        df_in = pd.DataFrame(
            {
                "col_01": ["a", "a", "c"],
                "col_02": ["b", "b", "d"],
                "col_03": [1, 2, 3],
            }
        ).astype(
            {
                "col_01": "category",
                "col_02": "category",
            }
        )

        df_out = pd.DataFrame({"col_01": ["a", "c"], "col_02": ["b", "d"], "col_03": [3, 3]}).astype(
            {
                "col_01": "category",
                "col_02": "category",
            }
        )

        df_exp = dataframes.groupby_agg(df_in, ["col_01", "col_02"]).reset_index()

        assert dataframes.are_equal(
            df1=df_exp,
            df2=df_out,
            verbose=True,
        )[0]


class TestMultiMerge:
    df1 = pd.DataFrame({"col_01": ["aa", "ab", "ac"], "col_02": ["ba", "bb", "bc"]})

    def test_merge_identical_dataframes(self):
        df1 = self.df1.copy()
        df2 = self.df1.copy()
        df3 = self.df1.copy()
        assert dataframes.multi_merge([df1, df2, df3], how="inner", on=["col_01", "col_02"]).equals(df1)

    def test_inner_join_with_non_overlapping_dataframes(self):
        df1 = self.df1.copy()
        df2 = pd.DataFrame({"col_01": ["ad", "ae"]})
        df3 = pd.DataFrame({"col_01": ["af"], "col_03": ["ca"]})
        # For some reason the order of columns changes on the second merge.
        df_out = pd.DataFrame({"col_02": [], "col_01": [], "col_03": []}, dtype=str)
        assert dataframes.are_equal(
            df1=dataframes.multi_merge([df1, df2, df3], how="inner", on="col_01"),
            df2=df_out,
        )  # type: ignore

    def test_outer_join_with_non_overlapping_dataframes(self):
        df1 = self.df1.copy()
        df2 = pd.DataFrame({"col_01": ["ad"]})
        df3 = pd.DataFrame({"col_01": ["ae"]})
        df_out = pd.DataFrame(
            {
                "col_01": ["aa", "ab", "ac", "ad", "ae"],
                "col_02": ["ba", "bb", "bc", np.nan, np.nan],
            }
        )
        assert dataframes.are_equal(
            df1=dataframes.multi_merge([df1, df2, df3], how="outer", on="col_01"),
            df2=df_out,
        )[0]

    def test_left_join(self):
        df1 = self.df1.copy()
        df2 = pd.DataFrame(
            {
                "col_01": ["aa", "ab", "ad"],
                "col_02": ["ba", "bB", "bc"],
                "col_03": [1, 2, 3],
            }
        )
        # df_12 = pd.DataFrame({'col_01': ['aa', 'ab', 'ac'], 'col_02': ['ba', 'bb', 'bc'],
        #                       'col_03': [1, np.nan, np.nan]})
        df3 = pd.DataFrame({"col_01": [], "col_02": [], "col_04": []})
        df_out = pd.DataFrame(
            {
                "col_01": ["aa", "ab", "ac"],
                "col_02": ["ba", "bb", "bc"],
                "col_03": [1, np.nan, np.nan],
                "col_04": [np.nan, np.nan, np.nan],
            }
        )
        assert dataframes.multi_merge([df1, df2, df3], how="left", on=["col_01", "col_02"]).equals(df_out)

    def test_right_join(self):
        df1 = self.df1.copy()
        df2 = pd.DataFrame(
            {
                "col_01": ["aa", "ab", "ad"],
                "col_02": ["ba", "bB", "bc"],
                "col_03": [1, 2, 3],
            }
        )
        # df12 = pd.DataFrame({'col_01': ['aa', 'ab', 'ad'], 'col_02': ['ba', 'bB', 'bc'], 'col_03': [1, 2, 3]})
        df3 = pd.DataFrame({"col_01": ["aa", "ae"], "col_02": ["ba", "be"], "col_04": [4, 5]})
        df_out = pd.DataFrame(
            {
                "col_01": ["aa", "ae"],
                "col_02": ["ba", "be"],
                "col_03": [1, np.nan],
                "col_04": [4, 5],
            }
        )
        assert dataframes.multi_merge([df1, df2, df3], how="right", on=["col_01", "col_02"]).equals(df_out)


class TestMapSeries:
    mapping = {
        "country_01": "Country 1",
        "country_02": "Country 2",
    }

    def test_all_countries_mapped_and_all_mappings_used(self):
        series_in = pd.Series(["country_01", "country_02"])
        series_out = pd.Series(["Country 1", "Country 2"])
        assert dataframes.map_series(series=series_in, mapping=self.mapping).equals(series_out)

    def test_one_country_missing_in_mapping(self):
        series_in = pd.Series(["country_01", "country_02", "country_03"])
        series_out = pd.Series(["Country 1", "Country 2", "country_03"])
        assert dataframes.map_series(series=series_in, mapping=self.mapping, make_unmapped_values_nan=False).equals(
            series_out
        )

    def test_one_country_missing_in_mapping_converted_into_nan(self):
        series_in = pd.Series(["country_01", "country_02", "country_03"])
        series_out = pd.Series(["Country 1", "Country 2", np.nan])
        assert dataframes.map_series(series=series_in, mapping=self.mapping, make_unmapped_values_nan=True).equals(
            series_out
        )

    def test_warn_if_one_country_missing_in_mapping(self):
        series_in = pd.Series(["country_01", "country_02", "country_03"])
        with warns(UserWarning, match="missing"):
            dataframes.map_series(series=series_in, mapping=self.mapping, warn_on_missing_mappings=True)

    def test_one_country_unused_in_mapping(self):
        series_in = pd.Series(["country_01"])
        series_out = pd.Series(["Country 1"])
        assert dataframes.map_series(series=series_in, mapping=self.mapping, warn_on_unused_mappings=False).equals(
            series_out
        )

    def test_warn_when_one_country_unused_in_mapping(self):
        series_in = pd.Series(["country_01"])
        with warns(UserWarning, match="unused"):
            dataframes.map_series(series=series_in, mapping=self.mapping, warn_on_unused_mappings=True)

    def test_empty_series(self):
        series_in = pd.Series([], dtype=object)
        series_out = pd.Series([], dtype=object)
        assert dataframes.map_series(series=series_in, mapping=self.mapping).equals(series_out)

    def test_empty_mapping(self):
        mapping = {}
        series_in = pd.Series(["country_01", "country_02"])
        series_out = pd.Series(["country_01", "country_02"])
        assert dataframes.map_series(series=series_in, mapping=mapping).equals(series_out)

    def test_mappings_of_mixed_types(self):
        # Note: A series containing 1 and True are considered identical. Therefore, a mapping
        # > pd.Series([1, 2, True]).map({1: 10, 2: 20, True: 30})
        # would result in
        # > pd.Series([30, 20, 30])
        # since 1 is considered identical to True, and the latest occurrence in the mapping prevails (namely True: 30).
        mapping = {2: "20", 3: False, "4": 40, True: 50}
        series_in = pd.Series([2, 3, "4", True])
        series_out = pd.Series(["20", False, 40, 50])
        assert dataframes.map_series(series=series_in, mapping=mapping).equals(series_out)

    def test_mapping_to_nan(self):
        # Even if make_unmapped_values_nan is False, we want to keep nan values if we are intentionally mapping a value
        # to nan. For example, if the mapping is {"bad_value": np.nan} we want to keep that nan.
        mapping = {2: 20, 3: np.nan}
        series_in = pd.Series([1, 2, 3])
        series_out = pd.Series([1, 20, np.nan])
        assert dataframes.map_series(series=series_in, mapping=mapping, make_unmapped_values_nan=False).equals(
            series_out
        )

    def test_map_categorical(self):
        series_in = pd.Series(["country_01", "country_02", "country_03", np.nan]).astype("category")
        series_out = pd.Series(["Country 1", "Country 2", "country_03", np.nan]).astype("category")
        out = dataframes.map_series(series=series_in, mapping=self.mapping, make_unmapped_values_nan=False)
        assert out.equals(series_out)
        assert out.dtype == "category"

    def test_map_categorical_non_unique(self):
        series_in = pd.Series(["country_01", "country_02", "country_03", np.nan]).astype("category")
        series_out = pd.Series(["Country 1", "Country 1", "country_03", np.nan]).astype("category")
        mapping = {
            "country_01": "Country 1",
            "country_02": "Country 1",
        }
        out = dataframes.map_series(series=series_in, mapping=mapping, make_unmapped_values_nan=False)
        assert out.equals(series_out)
        assert out.dtype == "category"

    def test_map_categorical_with_nan(self):
        series_in = pd.Series(["country_01", "country_02", "country_03", np.nan]).astype("category")
        mapping = {
            "country_01": "Country 1",
            "country_02": np.nan,
        }

        out = dataframes.map_series(series=series_in, mapping=mapping, make_unmapped_values_nan=False)
        assert out.equals(pd.Series(["Country 1", np.nan, "country_03", np.nan]).astype("category"))
        assert out.dtype == "category"

        out = dataframes.map_series(series=series_in, mapping=mapping, make_unmapped_values_nan=True)
        assert out.equals(pd.Series(["Country 1", np.nan, np.nan, np.nan]).astype("category"))
        assert out.dtype == "category"


class TestRenameCategories:
    def test_rename_categories(self):
        series_in = pd.Series(["country_01", "country_02", "country_03", np.nan]).astype("category")
        series_out = pd.Series(["Country 1", "Country 1", "country_03", np.nan]).astype("category")
        mapping = {
            "country_01": "Country 1",
            "country_02": "Country 1",
        }
        out = dataframes.rename_categories(series=series_in, mapping=mapping)
        assert out.equals(series_out)
        assert out.dtype == "category"

    def test_rename_categories_with_nans(self):
        series_in = pd.Series(["country_01", "country_02", "country_03", np.nan]).astype("category")
        series_out = pd.Series(["Country 1", np.nan, "country_03", np.nan]).astype("category")
        mapping = {
            "country_01": "Country 1",
            "country_02": np.nan,
        }
        out = dataframes.rename_categories(series=series_in, mapping=mapping)
        assert out.equals(series_out)
        assert out.dtype == "category"


class TestConcatenate:
    def test_concat_categoricals(self):
        a = pd.DataFrame({"x": ["a"], "d": [1]}).astype("category")
        b = pd.DataFrame({"x": ["b"], "d": [2]}).astype("category")

        out = dataframes.concatenate([a, b])
        assert list(out.x.cat.categories) == ["a", "b"]
        assert out.to_dict(orient="records") == [{"x": "a", "d": 1}, {"x": "b", "d": 2}]


class TestApplyOnCategoricals:
    def test_string_func(self):
        df = pd.DataFrame({"x": ["a", "b"], "y": ["b", "c"]}).astype("category")
        out = dataframes.apply_on_categoricals([df.x, df.y], lambda x, y: str(x + "|" + y))
        assert list(out.categories) == ["a|b", "b|c"]

    def test_mask(self):
        desc = pd.Series(["a", "b", np.nan]).astype("category")
        mask = pd.Series([False, True, True])

        new_desc = dataframes.apply_on_categoricals(
            [desc, mask.astype("category")],
            lambda desc, mask: f"{'' if pd.isna(desc) else desc} per capita" if mask else f"{desc}",
        )

        assert list(new_desc) == ["a", "b per capita", " per capita"]


class TestCombineTwoOverlappingDataFrames:
    def test_combine_dataframes(self):
        a = pd.DataFrame({"year": [2000, 2001, 2002], "var_a": [0, 1, 2], "var_b": ["0", "1", "2"]})
        b = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "var_a": [10, 20, 30],
                "var_c": ["10", "20", "30"],
            }
        )
        out = dataframes.combine_two_overlapping_dataframes(df1=a, df2=b, index_columns=["year"])
        expected = pd.DataFrame(
            {
                "year": [2000, 2001, 2002, 2003],
                "var_a": [0, 1, 2, 30],
                "var_b": ["0", "1", "2", np.nan],
                "var_c": [np.nan, "10", "20", "30"],
            }
        )

        assert dataframes.are_equal(df1=expected, df2=out)[0]

    def test_combine_dataframes_inverted_order(self):
        # Now we prioritize b.
        a = pd.DataFrame({"year": [2000, 2001, 2002], "var_a": [0, 1, 2], "var_b": ["0", "1", "2"]})
        b = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "var_a": [10, 20, 30],
                "var_c": ["10", "20", "30"],
            }
        )
        out = dataframes.combine_two_overlapping_dataframes(df1=b, df2=a, index_columns=["year"])
        expected = pd.DataFrame(
            {
                "year": [2000, 2001, 2002, 2003],
                "var_a": [0, 10, 20, 30],
                "var_b": ["0", "1", "2", np.nan],
                "var_c": [np.nan, "10", "20", "30"],
            }
        )

        assert dataframes.are_equal(df1=expected, df2=out)[0]

    def test_combine_dataframes_inverted_order_keeping_column_order(self):
        # Now we prioritize b.
        a = pd.DataFrame({"year": [2000, 2001, 2002], "var_a": [0, 1, 2], "var_b": ["0", "1", "2"]})
        b = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "var_a": [10, 20, 30],
                "var_c": ["10", "20", "30"],
            }
        )
        out = dataframes.combine_two_overlapping_dataframes(
            df1=b,
            df2=a,
            index_columns=["year"],
            keep_column_order=True,
        )
        expected = pd.DataFrame(
            {
                "year": [2000, 2001, 2002, 2003],
                "var_a": [0, 10, 20, 30],
                "var_c": [np.nan, "10", "20", "30"],
                "var_b": ["0", "1", "2", np.nan],
            }
        )

        assert dataframes.are_equal(df1=expected, df2=out)[0]

    def test_combine_dataframes_filling_nans_in_df1_with_df2(self):
        a = pd.DataFrame(
            {
                "year": [2000, 2001, 2002],
                "var_a": [0, np.nan, 2],
                "var_b": ["0", "1", "2"],
            }
        )
        b = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "var_a": [10, 20, 30],
                "var_c": ["10", "20", "30"],
            }
        )
        out = dataframes.combine_two_overlapping_dataframes(df1=a, df2=b, index_columns=["year"])
        expected = pd.DataFrame(
            {
                "year": [2000, 2001, 2002, 2003],
                "var_a": [0, 10, 2, 30],
                "var_b": ["0", "1", "2", np.nan],
                "var_c": [np.nan, "10", "20", "30"],
            }
        )

        assert dataframes.are_equal(df1=expected, df2=out)[0]

    def test_combine_dataframes_no_overlapping_columns(self):
        a = pd.DataFrame({"year": [2000, 2001, 2002], "var_a": [0, 1, 2], "var_b": ["0", "1", "2"]})
        b = pd.DataFrame({"year": [2001, 2002, 2003], "var_c": [10, 20, 30]})
        out = dataframes.combine_two_overlapping_dataframes(df1=a, df2=b, index_columns=["year"])
        expected = pd.DataFrame(
            {
                "year": [2000, 2001, 2002, 2003],
                "var_a": [0, 1, 2, np.nan],
                "var_b": ["0", "1", "2", np.nan],
                "var_c": [np.nan, 10, 20, 30],
            }
        )

        assert dataframes.are_equal(df1=expected, df2=out)[0]

    def test_combine_single_index_dataframes(self):
        a = pd.DataFrame({"year": [2000, 2001, 2002], "var_a": [0, 1, 2], "var_b": ["0", "1", "2"]}).set_index("year")
        b = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "var_a": [10, 20, 30],
                "var_c": ["10", "20", "30"],
            }
        ).set_index("year")
        out = dataframes.combine_two_overlapping_dataframes(df1=a, df2=b, index_columns=None)
        expected = pd.DataFrame(
            {
                "year": [2000, 2001, 2002, 2003],
                "var_a": [0, 1, 2, 30],
                "var_b": ["0", "1", "2", np.nan],
                "var_c": [np.nan, "10", "20", "30"],
            }
        ).set_index("year")

        assert dataframes.are_equal(df1=expected, df2=out)[0]

    def test_combine_multi_index_dataframes(self):
        a = pd.DataFrame(
            {
                "year": [2000, 2000, 2001, 2002],
                "country": ["country_b", "country_a", "country_a", "country_b"],
                "var_a": [0, 1, 2, 3],
                "var_b": ["0", "1", "2", "3"],
            }
        ).set_index(["year", "country"])
        b = pd.DataFrame(
            {
                "year": [2001, 2002, 2003],
                "country": ["country_a", "country_a", "country_b"],
                "var_a": [10, 20, 30],
                "var_c": ["10", "20", "30"],
            }
        ).set_index(["year", "country"])
        out = dataframes.combine_two_overlapping_dataframes(df1=a, df2=b, index_columns=None)
        # Note that currently combine_two_overlapping_dataframes sorts indexes (which at the moment can't be avoided).
        expected = pd.DataFrame(
            {
                "year": [2000, 2000, 2001, 2002, 2002, 2003],
                "country": [
                    "country_a",
                    "country_b",
                    "country_a",
                    "country_a",
                    "country_b",
                    "country_b",
                ],
                "var_a": [1, 0, 2, 20, 3, 30],
                "var_b": ["1", "0", "2", np.nan, "3", np.nan],
                "var_c": [np.nan, np.nan, "10", "20", np.nan, "30"],
            }
        ).set_index(["year", "country"])

        assert dataframes.are_equal(df1=expected, df2=out)[0]

    def test_warn_on_different_indexes(self):
        # In this case, combine_two_overlapping_dataframes will treat "a" as the same index as "aa", which is probably
        # never the desired behaviour. Therefore a warning must be raised.
        a = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).set_index("a")
        b = pd.DataFrame({"aa": [1, 2, 3], "bb": [4, 5, 6]}).set_index("aa")

        with warns(UserWarning, match="same indexes"):
            dataframes.combine_two_overlapping_dataframes(a, b, index_columns=None)

    def test_warn_if_index_columns_is_given_but_df1_has_index(self):
        a = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).set_index("a")
        b = pd.DataFrame({"a": [10, 20, 30], "b": [40, 50, 60]})

        with warns(UserWarning, match="dummy index"):
            dataframes.combine_two_overlapping_dataframes(a, b, index_columns=["b"])

    def test_warn_if_index_columns_is_given_but_df2_has_index(self):
        a = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        b = pd.DataFrame({"a": [10, 20, 30], "b": [40, 50, 60]}).set_index("a")

        with warns(UserWarning, match="dummy index"):
            dataframes.combine_two_overlapping_dataframes(a, b, index_columns=["b"])

    def test_warn_if_index_columns_is_given_but_both_dataframes_have_index(self):
        a = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).set_index("a")
        b = pd.DataFrame({"a": [10, 20, 30], "b": [40, 50, 60]}).set_index("a")

        with warns(UserWarning, match="dummy index"):
            dataframes.combine_two_overlapping_dataframes(a, b, index_columns=["b"])


class TestHasIndex:
    def test_dataframe_with_single_column_and_single_index(self):
        df = pd.DataFrame({"a": [1, 2, 3]}).set_index("a")
        assert dataframes.has_index(df=df) is True

    def test_dataframe_with_single_column_and_single_index_as_a_list(self):
        # This should not make a difference with the previous, but test it just in case.
        df = pd.DataFrame({"a": [1, 2, 3]}).set_index(["a"])
        assert dataframes.has_index(df=df) is True

    def test_dataframe_with_multiple_columns_and_single_index(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).set_index(["a"])
        assert dataframes.has_index(df=df) is True

    def test_dataframe_with_multiple_columns_and_multiple_indexes(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).set_index(["a", "b"])
        assert dataframes.has_index(df=df) is True

    def test_dataframe_with_multiple_columns_and_dummy_index(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        assert dataframes.has_index(df=df) is False

    def test_dataframe_with_multiple_columns_and_multiple_indexes_reset(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).set_index(["a", "b"]).reset_index()
        assert dataframes.has_index(df=df) is False

    def test_dataframe_with_multiple_columns_and_multiple_indexes_reset_dropping_index(
        self,
    ):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).set_index(["a", "b"]).reset_index(drop=True)
        assert dataframes.has_index(df=df) is False


def _store_dataframe_in_temp_file_and_read_it(df: pd.DataFrame, file_path: Path, **kwargs: Any) -> pd.DataFrame:
    with tempfile.TemporaryDirectory() as temp_dir:
        # Save dataframe in a temporary file.
        temp_file = Path(temp_dir) / file_path
        dataframes.to_file(df, file_path=temp_file, **kwargs)
        # Read dataframe.
        recovered_df = pd.read_csv(temp_file)

    return recovered_df


class TestToFile:
    def test_save_csv_file_with_dummy_index(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        recovered_df = _store_dataframe_in_temp_file_and_read_it(df=df, file_path=Path("temp.csv"))
        assert recovered_df.equals(df)

    def test_save_csv_file_with_dummy_index_and_overwrite(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save dataframe in a temporary file.
            temp_file = Path(temp_dir) / "temp.csv"
            dataframes.to_file(df, file_path=temp_file)
            # Overwrite dataframe.
            dataframes.to_file(df, file_path=temp_file)
            # Read dataframe.
            recovered_df = pd.read_csv(temp_file)
        assert recovered_df.equals(df)

    def test_save_csv_file_with_dummy_index_and_fail_to_overwrite(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save dataframe in a temporary file.
            temp_file = Path(temp_dir) / "temp.csv"
            dataframes.to_file(df, file_path=temp_file)
            with raises(FileExistsError):
                # Attempt to overwrite dataframe.
                dataframes.to_file(df, file_path=temp_file, overwrite=False)

    def test_save_csv_file_within_subfolders(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        # Save dataframe in a temporary file, ensuring all required subfolders are created.
        recovered_df = _store_dataframe_in_temp_file_and_read_it(df=df, file_path=Path("dir_1/dir_2/temp.csv"))
        assert recovered_df.equals(df)

    def test_save_csv_file_with_single_column_and_single_index(self):
        df = pd.DataFrame({"a": [1, 2, 3]}).set_index("a")
        recovered_df = _store_dataframe_in_temp_file_and_read_it(df=df, file_path=Path("temp.csv"))
        assert recovered_df.equals(df.reset_index())

    def test_save_csv_file_with_multiple_columns_and_single_index(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}).set_index("a")
        recovered_df = _store_dataframe_in_temp_file_and_read_it(df=df, file_path=Path("temp.csv"))
        assert recovered_df.equals(df.reset_index())

    def test_save_csv_file_with_multiple_columns_and_single_index_as_a_list(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}).set_index(["a"])
        recovered_df = _store_dataframe_in_temp_file_and_read_it(df=df, file_path=Path("temp.csv"))
        assert recovered_df.equals(df.reset_index())

    def test_save_csv_file_with_multiple_columns_and_multiple_indexes(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}).set_index(["a", "b"])
        recovered_df = _store_dataframe_in_temp_file_and_read_it(df=df, file_path=Path("temp.csv"))
        assert recovered_df.equals(df.reset_index())

    def test_save_csv_file_with_multiple_columns_and_multiple_indexes_ignoring_indexes(
        self,
    ):
        # Now the dataframe will be stored ignoring the index (so it will only have column "c").
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}).set_index(["a", "b"])
        recovered_df = _store_dataframe_in_temp_file_and_read_it(df=df, file_path=Path("temp.csv"), index=False)
        assert recovered_df.equals(df.reset_index(drop=True))

    def test_save_csv_file_with_multiple_columns_and_multiple_indexes_and_additional_kwarg(
        self,
    ):
        # We will impose that nans must be replaced by a certain number (by using keyword argument 'na_rep').
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, np.nan, 9]}).set_index(["a", "b"])
        recovered_df = _store_dataframe_in_temp_file_and_read_it(df=df, file_path=Path("temp.csv"), na_rep=80)
        assert recovered_df.equals(df.reset_index().replace({np.nan: 80}))

    def test_save_parquet_file_keeping_multiindex(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}).set_index(["a", "b"])
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save dataframe in a temporary file.
            temp_file = Path(temp_dir) / "test.parquet"
            dataframes.to_file(df, file_path=temp_file)
            # Read dataframe.
            recovered_df = pd.read_parquet(temp_file)
        assert recovered_df.equals(df)

    def test_save_parquet_file_resetting_multiindex(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}).set_index(["a", "b"])
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save dataframe in a temporary file.
            temp_file = Path(temp_dir) / "test.parquet"
            dataframes.to_file(df, file_path=temp_file, index=False)
            # Read dataframe.
            recovered_df = pd.read_parquet(temp_file)
        assert recovered_df.equals(df.reset_index(drop=True))

    def test_save_feather_file(self):
        # Multiindex dataframes cannot be stored as feather files.
        # Also, df.to_feather() does not accept an 'index' argument.
        # This test will check that 'index' is not been passed on to 'to_feather'.
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save dataframe in a temporary file.
            temp_file = Path(temp_dir) / "test.feather"
            dataframes.to_file(df, file_path=temp_file)
            # Read dataframe.
            recovered_df = pd.read_feather(temp_file)
        assert recovered_df.equals(df)

    def test_raise_error_on_unknown_file_extension(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save dataframe in a temporary file.
            temp_file = Path(temp_dir) / "test.made_up_extension"
            with raises(ValueError):
                dataframes.to_file(df, file_path=temp_file)

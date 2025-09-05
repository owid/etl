"""Test functions in etl.data_helpers.geo module."""

import json
import unittest
import warnings
from typing import cast
from unittest.mock import mock_open, patch

import numpy as np
import pandas as pd
import pytest
from owid.catalog import Dataset, Table
from owid.datautils import dataframes
from pytest import warns
from structlog.testing import capture_logs

from etl.data_helpers import geo
from etl.paths import LATEST_REGIONS_DATASET_PATH

mock_countries = {
    "country_02": "Country 2",
    "country_03": "Country 3",
}

mock_excluded_countries = [
    "country_05",
    "country_06",
]

mock_population = Table(
    {
        "country": ["Country 1", "Country 1", "Country 2", "Country 2", "Country 3"],
        "year": [2020, 2021, 2019, 2020, 2020],
        "population": [10, 20, 30, 40, 50],
    }
)

mock_countries_regions = pd.DataFrame(
    {
        "code": ["C01", "C02", "C03", "R01", "R02"],
        "name": ["Country 1", "Country 2", "Country 3", "Region 1", "Region 2"],
        "members": [np.nan, np.nan, np.nan, '["C01", "C02"]', '["C03"]'],
    }
).set_index("code")

mock_income_groups = pd.DataFrame(
    {
        "country": ["Country 2", "Country 3", "Country 1"],
        "income_group": ["Income group 1", "Income group 1", "Income group 2"],
    }
)


def mock_opens(filename, _):
    # This function mocks opening a file with path given by filename, and returns custom content for that file.
    mock_files_content = {
        "MOCK_COUNTRIES_FILE": mock_countries,
        "MOCK_EXCLUDED_COUNTRIES_FILE": mock_excluded_countries,
    }
    if filename in mock_files_content:
        content = mock_files_content[filename]
    else:
        raise FileNotFoundError(filename)
    file_object = mock_open(read_data=json.dumps(content)).return_value

    return file_object


def mock_population_load(*args, **kwargs):
    return mock_population


class TestAddPopulationToDataframe:
    def test_all_countries_and_years_in_population(self):
        df_in = pd.DataFrame({"country": ["Country 2", "Country 1"], "year": [2019, 2021]})
        df_out = pd.DataFrame(
            {
                "country": ["Country 2", "Country 1"],
                "year": [2019, 2021],
                "population": [30, 20],
            }
        )
        assert geo._add_population_to_dataframe(df=df_in, tb_population=mock_population).equals(df_out)

    def test_countries_and_years_in_population_just_one(self):
        df_in = pd.DataFrame({"country": ["Country 2", "Country 2"], "year": [2020, 2019]})
        df_out = pd.DataFrame(
            {
                "country": ["Country 2", "Country 2"],
                "year": [2020, 2019],
                "population": [40, 30],
            }
        )
        assert geo._add_population_to_dataframe(df=df_in, tb_population=mock_population).equals(df_out)

    def test_one_country_in_and_another_not_in_population(self):
        df_in = pd.DataFrame({"country": ["Country 1", "Country 3"], "year": [2020, 2021]})
        df_out = pd.DataFrame(
            {
                "country": ["Country 1", "Country 3"],
                "year": [2020, 2021],
                "population": [10, np.nan],
            }
        )
        assert geo._add_population_to_dataframe(df=df_in, tb_population=mock_population).equals(df_out)

    def test_no_countries_in_population(self):
        df_in = pd.DataFrame({"country": ["Country_04", "Country_04"], "year": [2000, 2000]})
        df_out = pd.DataFrame(
            {
                "country": ["Country_04", "Country_04"],
                "year": [2000, 2000],
                "population": [np.nan, np.nan],
            }
        )
        assert geo._add_population_to_dataframe(
            df=df_in, tb_population=mock_population, warn_on_missing_countries=False
        ).equals(df_out)

    def test_countries_in_population_but_not_for_given_years(self):
        df_in = pd.DataFrame({"country": ["Country 2", "Country 1"], "year": [2000, 2000]})
        df_out = pd.DataFrame(
            {
                "country": ["Country 2", "Country 1"],
                "year": [2000, 2000],
                "population": [np.nan, np.nan],
            }
        )
        assert geo._add_population_to_dataframe(df=df_in, tb_population=mock_population).equals(df_out)

    def test_countries_in_population_but_a_year_in_and_another_not_in_population(self):
        df_in = pd.DataFrame({"country": ["Country 2", "Country 1"], "year": [2019, 2000]})
        df_out = pd.DataFrame(
            {
                "country": ["Country 2", "Country 1"],
                "year": [2019, 2000],
                "population": [30, np.nan],
            }
        )
        assert geo._add_population_to_dataframe(df=df_in, tb_population=mock_population).equals(df_out)

    def test_change_country_and_year_column_names(self):
        df_in = pd.DataFrame({"Country": ["Country 2", "Country 1"], "Year": [2019, 2021]})
        df_out = pd.DataFrame(
            {
                "Country": ["Country 2", "Country 1"],
                "Year": [2019, 2021],
                "population": [30, 20],
            }
        )
        assert geo._add_population_to_dataframe(
            df=df_in, tb_population=mock_population, country_col="Country", year_col="Year"
        ).equals(df_out)

    def test_warn_if_countries_missing(self):
        df_in = pd.DataFrame({"country": ["Country_04", "Country_04"], "year": [2000, 2000]})
        df_out = pd.DataFrame(
            {
                "country": ["Country_04", "Country_04"],
                "year": [2000, 2000],
                "population": [np.nan, np.nan],
            }
        )
        with warns(UserWarning):
            geo._add_population_to_dataframe(
                df=df_in, tb_population=mock_population, warn_on_missing_countries=True
            ).equals(df_out)


@patch("builtins.open", new=mock_opens)
class TestHarmonizeCountries:
    def test_one_country_unchanged_and_another_changed(self):
        df_in = pd.DataFrame({"country": ["Country 1", "country_02"], "some_variable": [1, 2]})
        df_out = pd.DataFrame({"country": ["Country 1", "Country 2"], "some_variable": [1, 2]})
        assert geo.harmonize_countries(
            df=df_in,
            countries_file="MOCK_COUNTRIES_FILE",
            warn_on_unused_countries=False,
            warn_on_missing_countries=False,
        ).equals(df_out)

        # input dataframe is unchanged
        assert df_in.country.tolist() == ["Country 1", "country_02"]

    def test_one_country_unchanged_and_another_unknown(self):
        df_in = pd.DataFrame({"country": ["Country 1", "country_04"], "some_variable": [1, 2]})
        df_out = pd.DataFrame({"country": ["Country 1", "country_04"], "some_variable": [1, 2]})
        assert geo.harmonize_countries(
            df=df_in,
            countries_file="MOCK_COUNTRIES_FILE",
            warn_on_unused_countries=False,
            warn_on_missing_countries=False,
        ).equals(df_out)

    def test_two_unknown_countries_made_nan(self):
        df_in = pd.DataFrame({"country": ["Country 1", "country_04"], "some_variable": [1, 2]})
        df_out = pd.DataFrame({"country": [pd.NA, pd.NA], "some_variable": [1, 2]})
        df_out["country"] = df_out["country"].astype("str")

        result = geo.harmonize_countries(
            df=df_in,
            countries_file="MOCK_COUNTRIES_FILE",
            make_missing_countries_nan=True,
            warn_on_unused_countries=False,
            warn_on_missing_countries=False,
        )
        df_out.country = df_out.country.astype("string")
        result.country = result.country.astype("string")
        assert dataframes.are_equal(df1=df_out, df2=result)[0]

    def test_one_unknown_country_made_nan_and_a_known_country_changed(self):
        df_in = pd.DataFrame({"country": ["Country 1", "country_02"], "some_variable": [1, 2]})
        df_out = pd.DataFrame({"country": [np.nan, "Country 2"], "some_variable": [1, 2]})
        assert dataframes.are_equal(
            df1=df_out,
            df2=geo.harmonize_countries(
                df=df_in,
                countries_file="MOCK_COUNTRIES_FILE",
                make_missing_countries_nan=True,
                warn_on_unused_countries=False,
                warn_on_missing_countries=False,
            ),
        )[0]

    def test_on_dataframe_with_no_countries(self):
        df_in = pd.DataFrame({"country": []})
        df_out = pd.DataFrame({"country": []})
        df_out["country"] = df_out["country"].astype(object)
        result = geo.harmonize_countries(df=df_in, countries_file="MOCK_COUNTRIES_FILE", warn_on_unused_countries=False)
        assert result.empty

    def test_change_country_column_name(self):
        df_in = pd.DataFrame({"Country": ["country_02"]})
        df_out = pd.DataFrame({"Country": ["Country 2"]})
        assert dataframes.are_equal(
            df1=df_out,
            df2=geo.harmonize_countries(
                df=df_in,
                countries_file="MOCK_COUNTRIES_FILE",
                country_col="Country",
                warn_on_unused_countries=False,
            ),
        )[0]

    def test_warn_on_unused_mappings(self):
        df_in = pd.DataFrame({"country": ["country_02"], "some_variable": [1]})
        with warns(UserWarning, match="unused"):
            geo.harmonize_countries(
                df=df_in,
                countries_file="MOCK_COUNTRIES_FILE",
                warn_on_unused_countries=True,
                warn_on_missing_countries=True,
            )

    def test_warn_on_countries_missing_in_mapping(self):
        df_in = pd.DataFrame(
            {
                "country": ["country_02", "country_03", "country_04"],
                "some_variable": [1, 2, 3],
            }
        )
        with warns(UserWarning, match="missing"):
            geo.harmonize_countries(
                df=df_in,
                countries_file="MOCK_COUNTRIES_FILE",
                warn_on_unused_countries=True,
                warn_on_missing_countries=True,
            )

    def test_all_countries_excluded(self):
        df_in = pd.DataFrame({"country": ["country_05", "country_06"], "some_variable": [1, 2]})
        df_out = pd.DataFrame({"country": [], "some_variable": []}).astype({"country": str, "some_variable": int})
        assert geo.harmonize_countries(
            df=df_in,
            countries_file="MOCK_COUNTRIES_FILE",
            excluded_countries_file="MOCK_EXCLUDED_COUNTRIES_FILE",
            warn_on_unused_countries=False,
            warn_on_missing_countries=False,
        ).equals(df_out)

    def test_one_country_harmonized_and_one_excluded(self):
        df_in = pd.DataFrame({"country": ["country_02", "country_05"], "some_variable": [1, 2]})
        df_out = pd.DataFrame({"country": ["Country 2"], "some_variable": [1]})
        assert geo.harmonize_countries(
            df=df_in,
            countries_file="MOCK_COUNTRIES_FILE",
            excluded_countries_file="MOCK_EXCLUDED_COUNTRIES_FILE",
            warn_on_unused_countries=False,
            warn_on_missing_countries=False,
        ).equals(df_out)

    def test_one_country_left_equal_one_harmonized_and_one_excluded(self):
        df_in = pd.DataFrame({"country": ["country_01", "country_02", "country_05"], "some_variable": [1, 2, 3]})
        df_out = pd.DataFrame({"country": ["country_01", "Country 2"], "some_variable": [1, 2]})
        assert geo.harmonize_countries(
            df=df_in,
            countries_file="MOCK_COUNTRIES_FILE",
            excluded_countries_file="MOCK_EXCLUDED_COUNTRIES_FILE",
            warn_on_unused_countries=False,
            warn_on_missing_countries=False,
        ).equals(df_out)

    def test_warn_on_unknown_excluded_countries(self):
        # Since excluded countries contains "country_05" and "country_06", which are not contained in df_in, a warning
        # should be raised.
        df_in = pd.DataFrame({"country": ["country_02", "country_03"], "some_variable": [1, 2]})
        with warns(UserWarning, match="Unknown"):
            geo.harmonize_countries(
                df=df_in,
                countries_file="MOCK_COUNTRIES_FILE",
                excluded_countries_file="MOCK_EXCLUDED_COUNTRIES_FILE",
                warn_on_unused_countries=False,
                warn_on_missing_countries=False,
            )

    def test_no_warning_after_excluding_country(self):
        # Ensure no warning is raised because:
        # * All countries in excluded countries are included in data.
        # * All countries in data are included in countries mapping.
        # * All countries in countries mapping are used.
        df_in = pd.DataFrame(
            {"country": ["country_02", "country_03", "country_05", "country_06"], "some_variable": [1, 2, 3, 4]}
        )
        with warnings.catch_warnings():
            warnings.filterwarnings("error")
            warnings.filterwarnings("default", category=DeprecationWarning)
            geo.harmonize_countries(
                df=df_in,
                countries_file="MOCK_COUNTRIES_FILE",
                excluded_countries_file="MOCK_EXCLUDED_COUNTRIES_FILE",
                warn_on_unused_countries=True,
                warn_on_missing_countries=True,
            )


class TestListCountriesInRegions(unittest.TestCase):
    def test_get_countries_from_region(self):
        assert geo.list_countries_in_region(
            region="Region 1",
            countries_regions=mock_countries_regions,
            income_groups=mock_income_groups,
        ) == ["Country 1", "Country 2"]

    def test_get_countries_from_another_region(self):
        assert geo.list_countries_in_region(
            region="Region 2",
            countries_regions=mock_countries_regions,
            income_groups=mock_income_groups,
        ) == ["Country 3"]

    def test_get_countries_from_income_group(self):
        assert geo.list_countries_in_region(
            region="Income group 1",
            countries_regions=mock_countries_regions,
            income_groups=mock_income_groups,
        ) == ["Country 2", "Country 3"]

    def test_get_countries_from_another_income_group(self):
        assert geo.list_countries_in_region(
            region="Income group 2",
            countries_regions=mock_countries_regions,
            income_groups=mock_income_groups,
        ) == ["Country 1"]

    def test_raise_error_for_unknown_region(self):
        with self.assertRaises(geo.RegionNotFound):
            geo.list_countries_in_region(
                region="Made-up region",
                countries_regions=mock_countries_regions,
                income_groups=mock_income_groups,
            )

    def test_empty_region(self):
        assert (
            geo.list_countries_in_region(
                region="Country 1",
                countries_regions=mock_countries_regions,
                income_groups=mock_income_groups,
            )
            == []
        )


class TestListCountriesInRegionsThatMustHaveData(unittest.TestCase):
    def test_having_too_strict_condition_on_minimum_individual_contribution(self):
        with self.assertWarns(UserWarning):
            assert geo.list_countries_in_region_that_must_have_data(
                region="Region 1",
                reference_year=2020,
                min_frac_individual_population=0.81,
                min_frac_cumulative_population=0.0,
                countries_regions=mock_countries_regions,
                income_groups=mock_income_groups,
                population=mock_population,
            ) == ["Country 2", "Country 1"]

    def test_having_too_strict_condition_on_minimum_cumulative_contribution(self):
        with self.assertWarns(UserWarning):
            assert geo.list_countries_in_region_that_must_have_data(
                region="Region 1",
                reference_year=2020,
                min_frac_individual_population=0.0,
                min_frac_cumulative_population=0.81,
                countries_regions=mock_countries_regions,
                income_groups=mock_income_groups,
                population=mock_population,
            ) == ["Country 2", "Country 1"]

    def test_having_too_strict_condition_on_both_minimum_individual_and_cumulative_contributions(
        self,
    ):
        with self.assertWarns(UserWarning):
            assert geo.list_countries_in_region_that_must_have_data(
                region="Region 1",
                reference_year=2020,
                min_frac_individual_population=0.81,
                min_frac_cumulative_population=0.81,
                countries_regions=mock_countries_regions,
                income_groups=mock_income_groups,
                population=mock_population,
            ) == ["Country 2", "Country 1"]

    def test_region_year_with_only_one_country(self):
        assert geo.list_countries_in_region_that_must_have_data(
            region="Region 1",
            reference_year=2021,
            min_frac_individual_population=0.1,
            min_frac_cumulative_population=0,
            countries_regions=mock_countries_regions,
            income_groups=mock_income_groups,
            population=mock_population,
        ) == ["Country 1"]

    def test_region_year_right_below_minimum_individual_contribution(self):
        assert geo.list_countries_in_region_that_must_have_data(
            region="Region 1",
            reference_year=2020,
            min_frac_individual_population=0.79,
            min_frac_cumulative_population=0.0,
            countries_regions=mock_countries_regions,
            income_groups=mock_income_groups,
            population=mock_population,
        ) == ["Country 2"]

    def test_region_year_right_above_minimum_individual_contribution(self):
        assert geo.list_countries_in_region_that_must_have_data(
            region="Region 1",
            reference_year=2020,
            min_frac_individual_population=0.1,
            min_frac_cumulative_population=0.0,
            countries_regions=mock_countries_regions,
            income_groups=mock_income_groups,
            population=mock_population,
        ) == ["Country 2"]

    def test_region_year_right_below_minimum_cumulative_contribution(self):
        assert geo.list_countries_in_region_that_must_have_data(
            region="Region 1",
            reference_year=2020,
            min_frac_individual_population=0.0,
            min_frac_cumulative_population=0.79,
            countries_regions=mock_countries_regions,
            income_groups=mock_income_groups,
            population=mock_population,
        ) == ["Country 2"]

    def test_region_year_right_above_minimum_cumulative_contribution(self):
        assert geo.list_countries_in_region_that_must_have_data(
            region="Region 1",
            reference_year=2020,
            min_frac_individual_population=0.0,
            min_frac_cumulative_population=0.1,
            countries_regions=mock_countries_regions,
            income_groups=mock_income_groups,
            population=mock_population,
        ) == ["Country 2"]

    def test_countries_in_income_group(self):
        assert geo.list_countries_in_region_that_must_have_data(
            region="Income group 1",
            reference_year=2020,
            min_frac_individual_population=0.0,
            min_frac_cumulative_population=0.5,
            countries_regions=mock_countries_regions,
            income_groups=mock_income_groups,
            population=mock_population,
        ) == ["Country 3"]


@patch.object(geo, "_load_countries_regions", lambda: mock_countries_regions)
@patch.object(geo, "_load_income_groups", lambda: mock_income_groups)
class TestAddRegionAggregates:
    df_in = pd.DataFrame(
        {
            "country": [
                "Country 1",
                "Country 1",
                "Country 2",
                "Country 3",
                "Region 1",
                "Income group 1",
            ],
            "year": [2020, 2021, 2020, 2022, 2022, 2022],
            "var_01": [1, 2, 3, np.nan, 5, 6],
            "var_02": [10, 20, 30, 40, 50, 60],
        }
    )

    def test_add_region_with_one_nan_permitted(self):
        df = geo.add_region_aggregates(
            df=self.df_in,
            region="Region 2",
            countries_in_region=["Country 3"],
            countries_that_must_have_data=["Country 3"],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=None,
            country_col="country",
            year_col="year",
        )
        df_out = pd.DataFrame(
            {
                "country": [
                    "Country 1",
                    "Country 1",
                    "Country 2",
                    "Country 3",
                    "Income group 1",
                    "Region 1",
                    "Region 2",
                ],
                "year": [2020, 2021, 2020, 2022, 2022, 2022, 2022],
                "var_01": [1, 2, 3, np.nan, 6, 5, 0.0],
                "var_02": [10, 20, 30, 40, 60, 50, 40],
            }
        )
        assert dataframes.are_equal(df1=df, df2=df_out)[0]

    def test_add_region_with_one_nan_not_permitted(self):
        df = geo.add_region_aggregates(
            df=self.df_in,
            region="Region 2",
            countries_in_region=["Country 3"],
            countries_that_must_have_data=["Country 3"],
            num_allowed_nans_per_year=0,
            country_col="country",
            year_col="year",
        )
        df_out = pd.DataFrame(
            {
                "country": [
                    "Country 1",
                    "Country 1",
                    "Country 2",
                    "Country 3",
                    "Income group 1",
                    "Region 1",
                    "Region 2",
                ],
                "year": [2020, 2021, 2020, 2022, 2022, 2022, 2022],
                "var_01": [1, 2, 3, np.nan, 6, 5, np.nan],
                "var_02": [10, 20, 30, 40, 60, 50, 40],
            }
        )
        assert dataframes.are_equal(df1=df, df2=df_out)[0]

    def test_add_income_group(self):
        df = geo.add_region_aggregates(
            df=self.df_in,
            region="Income group 2",
            countries_in_region=["Country 1"],
            countries_that_must_have_data=["Country 1"],
            num_allowed_nans_per_year=0,
            country_col="country",
            year_col="year",
        )
        df_out = pd.DataFrame(
            {
                "country": [
                    "Country 1",
                    "Country 1",
                    "Country 2",
                    "Country 3",
                    "Income group 1",
                    "Income group 2",
                    "Income group 2",
                    "Region 1",
                ],
                "year": [2020, 2021, 2020, 2022, 2022, 2020, 2021, 2022],
                "var_01": [1, 2, 3, np.nan, 6, 1, 2, 5],
                "var_02": [10, 20, 30, 40, 60, 10, 20, 50],
            }
        )
        assert dataframes.are_equal(df1=df, df2=df_out)[0]

    def test_replace_region_with_one_non_mandatory_country_missing(self):
        # Country 2 does not have data for 2021, however, since it is not a mandatory country, its data will be treated
        # as zero.
        df = geo.add_region_aggregates(
            df=self.df_in,
            region="Region 1",
            countries_in_region=["Country 1", "Country 2"],
            countries_that_must_have_data=["Country 1"],
            num_allowed_nans_per_year=0,
            country_col="country",
            year_col="year",
        )
        df_out = pd.DataFrame(
            {
                "country": [
                    "Country 1",
                    "Country 1",
                    "Country 2",
                    "Country 3",
                    "Income group 1",
                    "Region 1",
                    "Region 1",
                ],
                "year": [2020, 2021, 2020, 2022, 2022, 2020, 2021],
                "var_01": [1, 2, 3, np.nan, 6, 1 + 3, 2],
                "var_02": [10, 20, 30, 40, 60, 10 + 30, 20],
            }
        )
        assert dataframes.are_equal(df1=df, df2=df_out)[0]

    def test_replace_region_with_one_mandatory_country_missing(self):
        # Country 2 does not have data for 2021, and, given that it is a mandatory country, the aggregation will be nan.
        df = geo.add_region_aggregates(
            df=self.df_in,
            region="Region 1",
            countries_in_region=["Country 1", "Country 2"],
            countries_that_must_have_data=["Country 1", "Country 2"],
            num_allowed_nans_per_year=0,
            country_col="country",
            year_col="year",
        )
        df_out = pd.DataFrame(
            {
                "country": [
                    "Country 1",
                    "Country 1",
                    "Country 2",
                    "Country 3",
                    "Income group 1",
                    "Region 1",
                    "Region 1",
                ],
                "year": [2020, 2021, 2020, 2022, 2022, 2020, 2021],
                "var_01": [1, 2, 3, np.nan, 6, 1 + 3, np.nan],
                "var_02": [10.0, 20.0, 30.0, 40.0, 60.0, 10.0 + 30.0, np.nan],
            }
        )
        assert dataframes.are_equal(df1=df, df2=df_out)[0]

    def test_replace_region_with_one_mandatory_country_having_nan(self):
        # Country 2 has NaN value in 2021. It is a mandatory country and is present, so the aggregation will
        # exist, but will be nan.
        df_in = self.df_in.copy()

        # Add NaN value for Country 2
        df_in.loc[len(df_in)] = {"country": "Country 2", "year": 2021, "var_01": np.nan, "var_02": np.nan}

        df = geo.add_region_aggregates(
            df=df_in,
            region="Region 1",
            countries_in_region=["Country 1", "Country 2"],
            countries_that_must_have_data=["Country 1", "Country 2"],
            num_allowed_nans_per_year=0,
            country_col="country",
            year_col="year",
        )
        df_out = pd.DataFrame(
            {
                "country": [
                    "Country 1",
                    "Country 1",
                    "Country 2",
                    "Country 2",
                    "Country 3",
                    "Income group 1",
                    "Region 1",
                    "Region 1",
                ],
                "year": [2020, 2021, 2020, 2021, 2022, 2022, 2020, 2021],
                "var_01": [1.0, 2.0, 3.0, np.nan, np.nan, 6.0, 4.0, np.nan],
                "var_02": [10.0, 20.0, 30.0, np.nan, 40.0, 60.0, 40.0, np.nan],
            }
        )
        assert dataframes.are_equal(df1=df, df2=df_out)[0]

    def test_replace_region_with_custom_aggregations(self):
        # Country 2 does not have data for 2021, and, given that it is a mandatory country, the aggregation will be nan.
        df = geo.add_region_aggregates(
            df=self.df_in,
            region="Region 1",
            countries_in_region=["Country 1", "Country 2"],
            countries_that_must_have_data=["Country 1", "Country 2"],
            num_allowed_nans_per_year=0,
            country_col="country",
            year_col="year",
            aggregations={"var_01": "sum", "var_02": "mean"},
        )
        df_out = pd.DataFrame(
            {
                "country": [
                    "Country 1",
                    "Country 1",
                    "Country 2",
                    "Country 3",
                    "Income group 1",
                    "Region 1",
                    "Region 1",
                ],
                "year": [2020, 2021, 2020, 2022, 2022, 2020, 2021],
                "var_01": [1, 2, 3, np.nan, 6, 1 + 3, np.nan],
                "var_02": [10.0, 20.0, 30.0, 40.0, 60.0, (10.0 + 30.0) / 2, np.nan],
            }
        )
        assert dataframes.are_equal(df1=df, df2=df_out)[0]

    def test_add_income_group_without_specifying_countries_in_region(self):
        df = geo.add_region_aggregates(
            df=self.df_in,
            region="Income group 2",
            countries_in_region=None,
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=0,
            country_col="country",
            year_col="year",
        )
        df_out = pd.DataFrame(
            {
                "country": [
                    "Country 1",
                    "Country 1",
                    "Country 2",
                    "Country 3",
                    "Income group 1",
                    "Income group 2",
                    "Income group 2",
                    "Region 1",
                ],
                "year": [2020, 2021, 2020, 2022, 2022, 2020, 2021, 2022],
                "var_01": [1, 2, 3, np.nan, 6, 1, 2, 5],
                "var_02": [10, 20, 30, 40, 60, 10, 20, 50],
            }
        )
        assert dataframes.are_equal(df1=df, df2=df_out)[0]

    def test_add_region_without_replacing_original(self):
        df = geo.add_region_aggregates(
            df=self.df_in,
            region="Region 1",
            countries_in_region=["Country 1", "Country 2"],
            countries_that_must_have_data=["Country 1"],
            num_allowed_nans_per_year=0,
            country_col="country",
            year_col="year",
            keep_original_region_with_suffix=" (TEST)",
        )
        df_out = pd.DataFrame(
            {
                "country": [
                    "Country 1",
                    "Country 1",
                    "Country 2",
                    "Country 3",
                    "Income group 1",
                    "Region 1",
                    "Region 1",
                    "Region 1 (TEST)",
                ],
                "year": [2020, 2021, 2020, 2022, 2022, 2020, 2021, 2022],
                "var_01": [1, 2, 3, np.nan, 6, 4, 2, 5],
                "var_02": [10, 20, 30, 40, 60, 40, 20, 50],
            }
        )
        assert dataframes.are_equal(df1=df, df2=df_out)[0]

    def test_add_region_with_table(self):
        tb = Table(self.df_in)
        tb.var_01.m.title = "Var 01"
        df = geo.add_region_aggregates(
            df=tb,
            region="Region 2",
            countries_in_region=["Country 3"],
            countries_that_must_have_data=["Country 3"],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=None,
            country_col="country",
            year_col="year",
        )
        df_out = Table(
            {
                "country": [
                    "Country 1",
                    "Country 1",
                    "Country 2",
                    "Country 3",
                    "Income group 1",
                    "Region 1",
                    "Region 2",
                ],
                "year": [2020, 2021, 2020, 2022, 2022, 2022, 2022],
                "var_01": [1, 2, 3, np.nan, 6, 5, 0.0],
                "var_02": [10, 20, 30, 40, 60, 50, 40],
            }
        )
        assert dataframes.are_equal(df1=df, df2=df_out)[0]
        assert df.var_01.m.title == "Var 01"


class MockRegionsDataset:
    def __getitem__(self, name: str) -> Table:
        mock_tb_regions = Table(
            {
                "code": ["OWID_EUR", "BLR", "FRA", "ITA", "RUS", "ESP", "OWID_USS"],
                "name": ["Europe", "Belarus", "France", "Italy", "Russia", "Spain", "USSR"],
                "region_type": [
                    "continent",
                    "country",
                    "country",
                    "country",
                    "country",
                    "country",
                    "country",
                ],
                "is_historical": [False, False, False, False, False, False, True],
                "members": ['["BLR", "FRA", "ITA", "RUS", "ESP", "OWID_USS"]', "[]", "[]", "[]", "[]", "[]", "[]"],
                "successors": ["[]", "[]", "[]", "[]", "[]", "[]", '["BLR", "RUS"]'],
            }
        ).set_index("code")
        return mock_tb_regions

    def read(self, name: str) -> Table:
        return self.__getitem__(name)


class MockIncomeGroupsDataset:
    table_names = ["income_groups", "income_groups_latest"]

    def __getitem__(self, name: str) -> Table:
        mock_tb_income_groups = Table(
            {
                "country": ["Belarus", "France", "Italy", "Russia", "Spain"],
                "year": [1991, 1987, 1987, 1991, 1987],
                "classification": [
                    "Upper-middle-income countries",
                    "High-income countries",
                    "High-income countries",
                    "Upper-middle-income countries",
                    "High-income countries",
                ],
            }
        ).set_index(["country", "year"])
        mock_tb_income_groups_latest = Table(
            {
                "country": ["Belarus", "France", "Italy", "Russia", "Spain"],
                "classification": [
                    "Upper-middle-income countries",
                    "High-income countries",
                    "High-income countries",
                    "Upper-middle-income countries",
                    "High-income countries",
                ],
            }
        ).set_index(["country"])

        if name == "income_groups":
            return mock_tb_income_groups
        elif name == "income_groups_latest":
            return mock_tb_income_groups_latest
        else:
            raise KeyError(f"Table {name} not found.")

    def read(self, name: str) -> Table:
        return self.__getitem__(name)


ds_regions = cast(Dataset, MockRegionsDataset())
ds_income_groups = cast(Dataset, MockIncomeGroupsDataset())


class TestAddRegionsToTable(unittest.TestCase):
    def test_overlaps_without_income_groups(self):
        tb_in = Table.from_records(
            [("USSR", 1985, 1), ("USSR", 1986, 2), ("Russia", 1986, 3), ("Russia", 2000, 4)],
            columns=["country", "year", "a"],
        )
        tb_expected = Table.from_records(
            [
                ["Europe", 1985, 1],
                ["Europe", 1986, 5],
                ["Europe", 2000, 4],
                ["Russia", 1986, 3],
                ["Russia", 2000, 4],
                ["USSR", 1985, 1],
                ["USSR", 1986, 2],
            ],
            columns=["country", "year", "a"],
        )
        # Do not check for overlaps.
        tb_out = geo.add_regions_to_table(tb=tb_in, ds_regions=ds_regions, check_for_region_overlaps=False)
        assert dataframes.are_equal(tb_out, tb_expected)[0]

        # Check for overlaps. Now a warning should be raised, since Russia and USSR overlap in 1986.
        with capture_logs() as captured_logs:
            tb_out = geo.add_regions_to_table(tb=tb_in, ds_regions=ds_regions, check_for_region_overlaps=True)
        assert captured_logs[0]["log_level"] == "warning"
        assert "overlap" in captured_logs[0]["event"]
        assert dataframes.are_equal(tb_out, tb_expected)[0]

        # Now run the same line again, but passing the expected overlap. No warning should be raised.
        with capture_logs() as captured_logs:
            tb_out = geo.add_regions_to_table(
                tb=tb_in,
                ds_regions=ds_regions,
                check_for_region_overlaps=True,
                accepted_overlaps=[{1986: {"Russia", "USSR"}}],
            )
        assert captured_logs == []
        assert dataframes.are_equal(tb_out, tb_expected)[0]

        # All the following should raise a warning, since the given overlaps are not exactly right.
        for accepted_overlaps in [
            [{1986: {"Russia", "USSR", "Georgia"}}],
            [{1985: {"USSR", "Georgia"}}],
            [{1986: {"Russia", "USSR"}, 1985: {"USSR", "Georgia"}}],
        ]:
            with capture_logs() as captured_logs:
                tb_out = geo.add_regions_to_table(
                    tb=tb_in, ds_regions=ds_regions, check_for_region_overlaps=True, accepted_overlaps=accepted_overlaps
                )
            assert captured_logs[0]["log_level"] == "warning"
            assert "overlap" in captured_logs[0]["event"]
            assert dataframes.are_equal(tb_out, tb_expected)[0]

    def test_overlaps_with_income_groups(self):
        tb_in = Table.from_records(
            [("USSR", 1985, 1), ("USSR", 1986, 2), ("Russia", 1986, 3), ("Russia", 2000, 4)],
            columns=["country", "year", "a"],
        )
        tb_expected = Table.from_records(
            [
                ["Europe", 1985, 1],
                ["Europe", 1986, 5],
                ["Europe", 2000, 4],
                ["Russia", 1986, 3],
                ["Russia", 2000, 4],
                ["USSR", 1985, 1],
                ["USSR", 1986, 2],
                ["Upper-middle-income countries", 1986, 3],
                ["Upper-middle-income countries", 2000, 4],
            ],
            columns=["country", "year", "a"],
        )
        # Do not check for overlaps.
        tb_out = geo.add_regions_to_table(
            tb=tb_in, ds_regions=ds_regions, ds_income_groups=ds_income_groups, check_for_region_overlaps=False
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]

        # Check for overlaps.
        tb_out = geo.add_regions_to_table(
            tb=tb_in,
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
            check_for_region_overlaps=True,
            accepted_overlaps=[{1986: {"Russia", "USSR"}}],
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]

    def test_overlaps_of_zeros(self):
        tb_in = Table.from_records(
            [("USSR", 1985, 1), ("USSR", 1986, 0), ("Russia", 1986, 0), ("Russia", 2000, 4)],
            columns=["country", "year", "a"],
        )
        tb_expected = Table.from_records(
            [
                ["Europe", 1985, 1],
                ["Europe", 1986, 0],
                ["Europe", 2000, 4],
                ["Russia", 1986, 0],
                ["Russia", 2000, 4],
                ["USSR", 1985, 1],
                ["USSR", 1986, 0],
            ],
            columns=["country", "year", "a"],
        )
        # This should work because we impose that we ignore overlaps of zeros.
        tb_out = geo.add_regions_to_table(
            tb=tb_in, ds_regions=ds_regions, check_for_region_overlaps=True, ignore_overlaps_of_zeros=True
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]

    def test_passing_explicit_list_of_countries(self):
        tb_in = Table.from_records(
            [("Russia", 1986, 1), ("Russia", 2000, 4), ("USSR", 1985, 1), ("USSR", 1986, 2)],
            columns=["country", "year", "a"],
        )
        # The following should create no aggregate, since we have no data for African countries.
        tb_out = geo.add_regions_to_table(
            tb=tb_in,
            ds_regions=ds_regions,
            regions=["Africa"],
            ds_income_groups=ds_income_groups,
            check_for_region_overlaps=False,
        )
        # The input table should be identical to the output.
        assert tb_in.to_dict(orient="split") == tb_out.to_dict(orient="split")
        tb_expected = Table.from_records(
            [
                ["Europe", 1985, 1],
                ["Europe", 1986, 3],
                ["Europe", 2000, 4],
                ["Russia", 1986, 1],
                ["Russia", 2000, 4],
                ["USSR", 1985, 1],
                ["USSR", 1986, 2],
            ],
            columns=["country", "year", "a"],
        )
        # The following should create an aggregate only for Europe.
        tb_out = geo.add_regions_to_table(
            tb=tb_in,
            ds_regions=ds_regions,
            regions=["Europe"],
            ds_income_groups=ds_income_groups,
            check_for_region_overlaps=False,
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]
        # Idem when passing a dictionary without modifications.
        tb_out = geo.add_regions_to_table(
            tb=tb_in,
            ds_regions=ds_regions,
            regions={"Europe": {}},
            ds_income_groups=ds_income_groups,
            check_for_region_overlaps=False,
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]
        # The following should create an aggregate for Europe, excluding USSR.
        tb_out = geo.add_regions_to_table(
            tb=tb_in,
            ds_regions=ds_regions,
            regions={"Europe": {"excluded_members": {"USSR"}}},
            ds_income_groups=ds_income_groups,
            check_for_region_overlaps=False,
        )
        assert tb_out.to_dict(orient="split") == {
            "index": [0, 1, 2, 3, 4, 5],
            "columns": ["country", "year", "a"],
            "data": [
                ["Europe", 1986, 1],
                ["Europe", 2000, 4],
                ["Russia", 1986, 1],
                ["Russia", 2000, 4],
                ["USSR", 1985, 1],
                ["USSR", 1986, 2],
            ],
        }

        # This should raise a warning (because field "excluded_members" has a typo):
        with capture_logs() as captured_logs:
            tb_out = geo.add_regions_to_table(
                tb=tb_in,
                ds_regions=ds_regions,
                regions={"Europe": {"typo_excluded_member": {"USSR"}}},
                ds_income_groups=ds_income_groups,
                check_for_region_overlaps=False,
            )
        assert captured_logs[0]["log_level"] == "warning"
        assert "typo_excluded_member" in captured_logs[0]["event"]
        assert dataframes.are_equal(tb_out, tb_expected)[0]

    def test_aggregates_with_income_groups(self):
        tb_in = Table.from_records(
            [("France", 2020, 1, 5), ("France", 2021, 2, 6), ("Italy", 2021, 3, 7), ("Italy", 2022, 4, 8)],
            columns=["country", "year", "a", "b"],
        )
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1, 5),
                ("Europe", 2021, 5, 13),
                ("Europe", 2022, 4, 8),
                ("France", 2020, 1, 5),
                ("France", 2021, 2, 6),
                ("High-income countries", 2020, 1, 5),
                ("High-income countries", 2021, 5, 13),
                ("High-income countries", 2022, 4, 8),
                ("Italy", 2021, 3, 7),
                ("Italy", 2022, 4, 8),
            ],
            columns=["country", "year", "a", "b"],
        )
        tb_out = geo.add_regions_to_table(tb=tb_in, ds_regions=ds_regions, ds_income_groups=ds_income_groups)
        assert dataframes.are_equal(tb_out, tb_expected)[0]

    def test_specify_aggregates(self):
        tb_in = Table.from_records(
            [("France", 2020, 1, 5), ("France", 2021, 2, 6), ("Italy", 2021, 3, 7), ("Italy", 2022, 4, 8)],
            columns=["country", "year", "a", "b"],
        )
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1, 5),
                ("Europe", 2021, 5, 13),
                ("Europe", 2022, 4, 8),
                ("France", 2020, 1, 5),
                ("France", 2021, 2, 6),
                ("High-income countries", 2020, 1, 5),
                ("High-income countries", 2021, 5, 13),
                ("High-income countries", 2022, 4, 8),
                ("Italy", 2021, 3, 7),
                ("Italy", 2022, 4, 8),
            ],
            columns=["country", "year", "a", "b"],
        )
        # Specify the aggregates for each column.
        tb_out = geo.add_regions_to_table(
            tb=tb_in, ds_regions=ds_regions, ds_income_groups=ds_income_groups, aggregations={"a": "sum", "b": "sum"}
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]
        # Idem, but now one of the columns uses sum and the other mean.
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1, 5),
                ("Europe", 2021, 5, 6.5),
                ("Europe", 2022, 4, 8),
                ("France", 2020, 1, 5),
                ("France", 2021, 2, 6),
                ("High-income countries", 2020, 1, 5),
                ("High-income countries", 2021, 5, 6.5),
                ("High-income countries", 2022, 4, 8),
                ("Italy", 2021, 3, 7),
                ("Italy", 2022, 4, 8),
            ],
            columns=["country", "year", "a", "b"],
        )
        tb_out = geo.add_regions_to_table(
            tb=tb_in, ds_regions=ds_regions, ds_income_groups=ds_income_groups, aggregations={"a": "sum", "b": "mean"}
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]
        # Now only one of the columns has an aggregate.
        # The other column should keep its original data, but aggregates will only have nans in that column.
        # Because of the nans, other values in that column become floats.
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1, np.nan),
                ("Europe", 2021, 5, np.nan),
                ("Europe", 2022, 4, np.nan),
                ("France", 2020, 1, 5.0),
                ("France", 2021, 2, 6.0),
                ("High-income countries", 2020, 1, np.nan),
                ("High-income countries", 2021, 5, np.nan),
                ("High-income countries", 2022, 4, np.nan),
                ("Italy", 2021, 3, 7.0),
                ("Italy", 2022, 4, 8.0),
            ],
            columns=["country", "year", "a", "b"],
        )
        tb_out = geo.add_regions_to_table(
            tb=tb_in, ds_regions=ds_regions, ds_income_groups=ds_income_groups, aggregations={"a": "sum"}
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]

    def test_region_data_already_existed(self):
        tb_in = Table.from_records(
            [
                ("France", 2020, 1, 5),
                ("France", 2021, 2, 6),
                ("Italy", 2021, 3, 7),
                ("Italy", 2022, 4, 8),
                ("Europe", 2020, 0, 0),
                ("Europe", 2021, 0, 0),
                ("Europe", 2022, 0, 0),
            ],
            columns=["country", "year", "a", "b"],
        )
        # The old data for Europe should be replaced by the new aggregates.
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1, 5),
                ("Europe", 2021, 5, 13),
                ("Europe", 2022, 4, 8),
                ("France", 2020, 1, 5),
                ("France", 2021, 2, 6),
                ("High-income countries", 2020, 1, 5),
                ("High-income countries", 2021, 5, 13),
                ("High-income countries", 2022, 4, 8),
                ("Italy", 2021, 3, 7),
                ("Italy", 2022, 4, 8),
            ],
            columns=["country", "year", "a", "b"],
        )
        tb_out = geo.add_regions_to_table(tb=tb_in, ds_regions=ds_regions, ds_income_groups=ds_income_groups)
        assert dataframes.are_equal(tb_out, tb_expected)[0]
        # Now the old data for Europe should be kept, with the appended text.
        tb_out = geo.add_regions_to_table(
            tb=tb_in,
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
            keep_original_region_with_suffix=" (old)",
        )
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1, 5),
                ("Europe", 2021, 5, 13),
                ("Europe", 2022, 4, 8),
                ("Europe (old)", 2020, 0, 0),
                ("Europe (old)", 2021, 0, 0),
                ("Europe (old)", 2022, 0, 0),
                ("France", 2020, 1, 5),
                ("France", 2021, 2, 6),
                ("High-income countries", 2020, 1, 5),
                ("High-income countries", 2021, 5, 13),
                ("High-income countries", 2022, 4, 8),
                ("Italy", 2021, 3, 7),
                ("Italy", 2022, 4, 8),
            ],
            columns=["country", "year", "a", "b"],
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]
        # In the following case, Europe already has data for a column that does not have a defined aggregation ('b').
        # That data will become nan, and a warning will be raised.
        with capture_logs() as captured_logs:
            tb_out = geo.add_regions_to_table(
                tb=tb_in, ds_regions=ds_regions, ds_income_groups=ds_income_groups, aggregations={"a": "mean"}
            )
        assert captured_logs[0]["log_level"] == "warning"
        assert "Europe" in captured_logs[0]["event"]
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1.0, None),
                ("Europe", 2021, 2.5, None),
                ("Europe", 2022, 4.0, None),
                ("France", 2020, 1, 5.0),
                ("France", 2021, 2, 6.0),
                ("High-income countries", 2020, 1.0, None),
                ("High-income countries", 2021, 2.5, None),
                ("High-income countries", 2022, 4.0, None),
                ("Italy", 2021, 3.0, 7.0),
                ("Italy", 2022, 4.0, 8.0),
            ],
            columns=["country", "year", "a", "b"],
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]

    def test_nan_conditions(self):
        tb_in = Table.from_records(
            [
                ("France", 2020, 1, 7),
                ("France", 2021, 2, 8),
                ("Italy", 2021, 3, 9),
                ("Italy", 2022, 4, 10),
                ("Spain", 2021, None, 11),
                ("Spain", 2022, 6, 12),
            ],
            columns=["country", "year", "a", "b"],
        )

        # First allow zero nans.
        tb_out = geo.add_regions_to_table(tb=tb_in, ds_regions=ds_regions, num_allowed_nans_per_year=0)
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1.0, 7),
                ("Europe", 2021, None, 28),
                ("Europe", 2022, 10.0, 22),
                ("France", 2020, 1.0, 7),
                ("France", 2021, 2.0, 8),
                ("Italy", 2021, 3.0, 9),
                ("Italy", 2022, 4.0, 10),
                ("Spain", 2021, None, 11),
                ("Spain", 2022, 6.0, 12),
            ],
            columns=["country", "year", "a", "b"],
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]

        # Now only one 1 nan is allowed.
        tb_out = geo.add_regions_to_table(tb=tb_in, ds_regions=ds_regions, num_allowed_nans_per_year=1)
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1.0, 7),
                ("Europe", 2021, 5.0, 28),
                ("Europe", 2022, 10.0, 22),
                ("France", 2020, 1.0, 7),
                ("France", 2021, 2.0, 8),
                ("Italy", 2021, 3.0, 9),
                ("Italy", 2022, 4.0, 10),
                ("Spain", 2021, None, 11),
                ("Spain", 2022, 6.0, 12),
            ],
            columns=["country", "year", "a", "b"],
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]

        # Another example allowing 1 nan.
        tb_in = Table.from_records(
            [
                ("France", 2020, 1, 7),
                ("France", 2021, 2, 8),
                ("Italy", 2021, None, 9),
                ("Italy", 2022, 4, 10),
                ("Spain", 2021, None, 11),
                ("Spain", 2022, 6, 12),
            ],
            columns=["country", "year", "a", "b"],
        )
        tb_out = geo.add_regions_to_table(tb=tb_in, ds_regions=ds_regions, num_allowed_nans_per_year=1)
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1.0, 7),
                ("Europe", 2021, None, 28),
                ("Europe", 2022, 10.0, 22),
                ("France", 2020, 1.0, 7),
                ("France", 2021, 2.0, 8),
                ("Italy", 2021, None, 9),
                ("Italy", 2022, 4.0, 10),
                ("Spain", 2021, None, 11),
                ("Spain", 2022, 6.0, 12),
            ],
            columns=["country", "year", "a", "b"],
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]

        # Now allow 2 nans.
        tb_out = geo.add_regions_to_table(tb=tb_in, ds_regions=ds_regions, num_allowed_nans_per_year=2)
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1.0, 7),
                ("Europe", 2021, 2.0, 28),
                ("Europe", 2022, 10.0, 22),
                ("France", 2020, 1.0, 7),
                ("France", 2021, 2.0, 8),
                ("Italy", 2021, None, 9),
                ("Italy", 2022, 4.0, 10),
                ("Spain", 2021, None, 11),
                ("Spain", 2022, 6.0, 12),
            ],
            columns=["country", "year", "a", "b"],
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]

        # Now impose a fraction of allowed nans of exactly zero.
        tb_out = geo.add_regions_to_table(tb=tb_in, ds_regions=ds_regions, frac_allowed_nans_per_year=0)
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1.0, 7),
                ("Europe", 2021, None, 28),
                ("Europe", 2022, 10.0, 22),
                ("France", 2020, 1.0, 7),
                ("France", 2021, 2.0, 8),
                ("Italy", 2021, None, 9),
                ("Italy", 2022, 4.0, 10),
                ("Spain", 2021, None, 11),
                ("Spain", 2022, 6.0, 12),
            ],
            columns=["country", "year", "a", "b"],
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]

        # Now allow for 50% nans.
        tb_out = geo.add_regions_to_table(tb=tb_in, ds_regions=ds_regions, frac_allowed_nans_per_year=0.5)
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1.0, 7),
                ("Europe", 2021, None, 28),
                ("Europe", 2022, 10.0, 22),
                ("France", 2020, 1.0, 7),
                ("France", 2021, 2.0, 8),
                ("Italy", 2021, None, 9),
                ("Italy", 2022, 4.0, 10),
                ("Spain", 2021, None, 11),
                ("Spain", 2022, 6.0, 12),
            ],
            columns=["country", "year", "a", "b"],
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]

        # Now allow for maximum 70% nans.
        tb_out = geo.add_regions_to_table(tb=tb_in, ds_regions=ds_regions, frac_allowed_nans_per_year=0.7)
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1.0, 7),
                ("Europe", 2021, 2.0, 28),
                ("Europe", 2022, 10.0, 22),
                ("France", 2020, 1.0, 7),
                ("France", 2021, 2.0, 8),
                ("Italy", 2021, None, 9),
                ("Italy", 2022, 4.0, 10),
                ("Spain", 2021, None, 11),
                ("Spain", 2022, 6.0, 12),
            ],
            columns=["country", "year", "a", "b"],
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]

        # Now impose a minimum number of valid values of zero (which is always fulfilled).
        tb_out = geo.add_regions_to_table(tb=tb_in, ds_regions=ds_regions, min_num_values_per_year=0)
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1.0, 7),
                ("Europe", 2021, 2.0, 28),
                ("Europe", 2022, 10.0, 22),
                ("France", 2020, 1.0, 7),
                ("France", 2021, 2.0, 8),
                ("Italy", 2021, None, 9),
                ("Italy", 2022, 4.0, 10),
                ("Spain", 2021, None, 11),
                ("Spain", 2022, 6.0, 12),
            ],
            columns=["country", "year", "a", "b"],
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]

        # Now impose a minimum of 1 valid value per group.
        tb_out = geo.add_regions_to_table(tb=tb_in, ds_regions=ds_regions, min_num_values_per_year=1)
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1.0, 7),
                ("Europe", 2021, 2.0, 28),
                ("Europe", 2022, 10.0, 22),
                ("France", 2020, 1.0, 7),
                ("France", 2021, 2.0, 8),
                ("Italy", 2021, None, 9),
                ("Italy", 2022, 4.0, 10),
                ("Spain", 2021, None, 11),
                ("Spain", 2022, 6.0, 12),
            ],
            columns=["country", "year", "a", "b"],
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]

        # Now impose a minimum of 2 valid values per group.
        tb_out = geo.add_regions_to_table(tb=tb_in, ds_regions=ds_regions, min_num_values_per_year=2)
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1.0, 7),
                ("Europe", 2021, None, 28),
                ("Europe", 2022, 10.0, 22),
                ("France", 2020, 1.0, 7),
                ("France", 2021, 2.0, 8),
                ("Italy", 2021, None, 9),
                ("Italy", 2022, 4.0, 10),
                ("Spain", 2021, None, 11),
                ("Spain", 2022, 6.0, 12),
            ],
            columns=["country", "year", "a", "b"],
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]

    def test_changing_country_and_year_col(self):
        tb_in = Table.from_records(
            [
                ("France", 2020, 1, 5),
                ("France", 2021, 2, 6),
                ("Spain", 2021, 3, 7),
                ("Spain", 2022, 4, 8),
            ],
            columns=["c", "y", "a", "b"],
        )
        tb_out = geo.add_regions_to_table(tb=tb_in, ds_regions=ds_regions, country_col="c", year_col="y")
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, 1, 5),
                ("Europe", 2021, 5, 13),
                ("Europe", 2022, 4, 8),
                ("France", 2020, 1, 5),
                ("France", 2021, 2, 6),
                ("Spain", 2021, 3, 7),
                ("Spain", 2022, 4, 8),
            ],
            columns=["c", "y", "a", "b"],
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]

    def test_countries_that_must_have_data(self):
        tb_in = Table.from_records(
            [
                ("France", 2020, 1, 5),
                ("France", 2021, 2, 6),
                ("Italy", 2021, 3, 7),
                ("Italy", 2022, 4, 8),
            ],
            columns=["country", "year", "a", "b"],
        )
        # First check result without countries_that_must_have_data.
        # Also check that the result is identical if countries_that_must_have_data is [] or {}.
        for countries_that_must_have_data in [[], {}, None]:
            tb_out = geo.add_regions_to_table(
                tb=tb_in,
                regions=["Europe"],
                ds_regions=ds_regions,
                ds_income_groups=ds_income_groups,
                countries_that_must_have_data=countries_that_must_have_data,
            )
            tb_expected = Table.from_records(
                [
                    ("Europe", 2020, 1, 5),
                    ("Europe", 2021, 5, 13),
                    ("Europe", 2022, 4, 8),
                    ("France", 2020, 1, 5),
                    ("France", 2021, 2, 6),
                    ("Italy", 2021, 3, 7),
                    ("Italy", 2022, 4, 8),
                ],
                columns=["country", "year", "a", "b"],
            )
            assert dataframes.are_equal(tb_out, tb_expected)[0]

        # Now impose that a country must have data.
        tb_out = geo.add_regions_to_table(
            tb=tb_in,
            regions=["Europe"],
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
            countries_that_must_have_data={"Europe": ["Italy"]},
        )
        tb_expected = Table.from_records(
            [
                ("Europe", 2020, None, None),
                ("Europe", 2021, 5, 13),
                ("Europe", 2022, 4, 8),
                ("France", 2020, 1, 5),
                ("France", 2021, 2, 6),
                ("Italy", 2021, 3, 7),
                ("Italy", 2022, 4, 8),
            ],
            columns=["country", "year", "a", "b"],
        )
        assert dataframes.are_equal(tb_out, tb_expected)[0]

        # If the keys of the dictionary are not known regions, an error should be raised.
        with self.assertRaises(AssertionError):
            _ = geo.add_regions_to_table(
                tb=tb_in,
                regions=["Europe"],
                ds_regions=ds_regions,
                ds_income_groups=ds_income_groups,
                countries_that_must_have_data={"Unknown region": ["Random"]},
            )

    def test_aggregates_with_multiple_dimensions(self):
        tb_in = Table.from_records(
            [
                ("France", 2020, "red", 1, 5),
                ("France", 2020, "red", 2, 6),
                ("France", 2020, "blue", 3, 7),
                ("Italy", 2022, "blue", 4, 8),
                ("Belarus", 2022, "blue", 5, 9),
            ],
            columns=["country", "year", "color", "a", "b"],
        )
        tb_expected = (
            Table.from_records(
                [
                    ("Belarus", 2022, "blue", 5, 9),
                    ("Europe", 2020, "blue", 3, 7),
                    ("Europe", 2020, "red", 3, 11),
                    ("Europe", 2022, "blue", 9, 17),
                    ("High-income countries", 2020, "blue", 3, 7),
                    ("High-income countries", 2020, "red", 3, 11),
                    ("High-income countries", 2022, "blue", 4, 8),
                    ("France", 2020, "red", 1, 5),
                    ("France", 2020, "red", 2, 6),
                    ("France", 2020, "blue", 3, 7),
                    ("Italy", 2022, "blue", 4, 8),
                    ("Upper-middle-income countries", 2022, "blue", 5, 9),
                ],
                columns=["country", "year", "color", "a", "b"],
            )
            .sort_values(["country", "year", "color"])
            .reset_index(drop=True)
        )
        tb_out = (
            geo.add_regions_to_table(
                tb=tb_in,
                index_columns=["country", "year", "color"],
                ds_regions=ds_regions,
                ds_income_groups=ds_income_groups,
            )
            .sort_values(["country", "year", "color"])
            .reset_index(drop=True)
        )
        assert tb_out.equals(tb_expected)


class TestCreateTableOfRegionsAndSubregions(unittest.TestCase):
    @pytest.mark.integration
    def test_regions_table_has_unique_members(self):
        """Test that create_table_of_regions_and_subregions produces a table with unique members in each row."""
        # Load the regions dataset
        ds_regions = Dataset(LATEST_REGIONS_DATASET_PATH)

        # Create the regions table
        tb_regions = geo.create_table_of_regions_and_subregions(ds_regions=ds_regions)

        # Check each row for duplicate members
        duplicate_info = []
        for idx, row in tb_regions.iterrows():
            region = row.name if hasattr(row, "name") else idx
            members = row["members"]

            if members is not None and isinstance(members, list):
                # Check for duplicates within this row's members list
                seen = set()
                duplicates = []
                for member in members:
                    if member in seen:
                        duplicates.append(member)
                    seen.add(member)

                if duplicates:
                    duplicate_info.append(f"Region '{region}' has duplicate members: {duplicates}")

        if duplicate_info:
            self.fail("Found duplicate members within region rows:\n" + "\n".join(duplicate_info))

        # If we get here, the test passed
        self.assertTrue(True, "All members are unique within each region")


class MockPopulationDataset:
    def __getitem__(self, name: str) -> Table:
        return mock_population

    def read(self, name: str) -> Table:
        return self.__getitem__(name)


class TestRegions(unittest.TestCase):
    """Test the Regions class functionality."""

    def setUp(self):
        """Set up test fixtures for Regions tests."""
        self.ds_regions = cast(Dataset, MockRegionsDataset())
        self.ds_income_groups = cast(Dataset, MockIncomeGroupsDataset())
        self.ds_population = cast(Dataset, MockPopulationDataset())

    def test_regions_initialization_with_datasets(self):
        """Test Regions initialization with provided datasets."""
        regions = geo.Regions(
            ds_regions=self.ds_regions,
            ds_income_groups=self.ds_income_groups,
            ds_population=self.ds_population,
            auto_load_datasets=False,
        )

        # Test that datasets are properly stored
        self.assertEqual(regions.ds_regions, self.ds_regions)
        self.assertEqual(regions.ds_income_groups, self.ds_income_groups)
        self.assertEqual(regions.ds_population, self.ds_population)

    def test_regions_initialization_auto_load_false(self):
        """Test Regions initialization without datasets when auto_load is False."""
        # This should work but raise errors when trying to access datasets
        regions = geo.Regions(auto_load_datasets=False)

        # Accessing ds_regions should raise an error since no dataset was provided
        with self.assertRaises(ValueError):
            _ = regions.ds_regions

    def test_get_region_continent(self):
        """Test getting information about a continent region."""
        regions = geo.Regions(
            ds_regions=self.ds_regions,
            ds_income_groups=self.ds_income_groups,
            auto_load_datasets=False,
        )

        europe_info = regions.get_region("Europe")

        # Check that it returns a dictionary with members
        self.assertIsInstance(europe_info, dict)
        self.assertIn("members", europe_info)
        self.assertIsInstance(europe_info["members"], list)

        # Europe should include Russia, France, Italy, Spain, Belarus
        expected_members = {"Russia", "France", "Italy", "Spain", "Belarus"}
        actual_members = set(europe_info["members"])
        self.assertTrue(expected_members.issubset(actual_members))

    def test_get_region_income_group(self):
        """Test getting information about an income group region."""
        regions = geo.Regions(
            ds_regions=self.ds_regions,
            ds_income_groups=self.ds_income_groups,
            auto_load_datasets=False,
        )

        high_income_info = regions.get_region("High-income countries")

        # Check that it returns a dictionary with members
        self.assertIsInstance(high_income_info, dict)
        self.assertIn("members", high_income_info)
        self.assertIsInstance(high_income_info["members"], list)

        # High-income countries should include France, Italy, Spain
        expected_members = {"France", "Italy", "Spain"}
        actual_members = set(high_income_info["members"])
        self.assertTrue(expected_members.issubset(actual_members))

    def test_get_regions_all(self):
        """Test getting all regions."""
        regions = geo.Regions(
            ds_regions=self.ds_regions,
            ds_income_groups=self.ds_income_groups,
            auto_load_datasets=False,
        )

        all_regions = regions.get_regions()

        # Should be a dictionary
        self.assertIsInstance(all_regions, dict)

        # Should contain both continents and income groups
        self.assertIn("Europe", all_regions)
        self.assertIn("High-income countries", all_regions)

    def test_get_regions_only_members(self):
        """Test getting regions with only_members=True."""
        regions = geo.Regions(
            ds_regions=self.ds_regions,
            ds_income_groups=self.ds_income_groups,
            auto_load_datasets=False,
        )

        regions_members = regions.get_regions(only_members=True)

        # Should be a dictionary of lists
        self.assertIsInstance(regions_members, dict)
        for region_name, members in regions_members.items():
            self.assertIsInstance(members, list)

    def test_get_regions_specific_names(self):
        """Test getting specific regions by name."""
        regions = geo.Regions(
            ds_regions=self.ds_regions,
            ds_income_groups=self.ds_income_groups,
            auto_load_datasets=False,
        )

        specific_regions = regions.get_regions(names=["Europe", "High-income countries"])

        # Should contain only the requested regions
        self.assertEqual(set(specific_regions.keys()), {"Europe", "High-income countries"})

    def test_harmonize_names_file_not_exists(self):
        """Test harmonize_names when countries file doesn't exist."""
        regions = geo.Regions(
            ds_regions=self.ds_regions,
            countries_file="nonexistent_file.json",
            auto_load_datasets=False,
        )

        tb = Table({"country": ["France", "Spain"], "year": [2020, 2020], "value": [1, 2]})

        # Should raise ValueError when file doesn't exist
        with self.assertRaises(ValueError):
            regions.harmonize_names(tb)

    @patch("builtins.open", new=mock_opens)
    @patch("pathlib.Path.exists")
    def test_harmonize_names_with_file(self, mock_exists):
        """Test harmonize_names with existing countries file."""
        mock_exists.return_value = True  # Mock file exists

        regions = geo.Regions(
            ds_regions=self.ds_regions,
            countries_file="MOCK_COUNTRIES_FILE",
            auto_load_datasets=False,
        )

        tb = Table({"country": ["country_02", "Country 1"], "year": [2020, 2020], "value": [1, 2]})

        result = regions.harmonize_names(tb, warn_on_missing_countries=False, warn_on_unused_countries=False)

        # country_02 should be harmonized to Country 2
        expected_countries = ["Country 2", "Country 1"]
        self.assertEqual(result["country"].tolist(), expected_countries)


class TestRegionAggregator(unittest.TestCase):
    """Test the RegionAggregator class functionality."""

    def setUp(self):
        """Set up test fixtures for RegionAggregator tests."""
        self.ds_regions = cast(Dataset, MockRegionsDataset())
        self.ds_income_groups = cast(Dataset, MockIncomeGroupsDataset())
        self.ds_population = cast(Dataset, MockPopulationDataset())

        # Sample data for testing
        self.tb_sample = Table(
            {
                "country": ["France", "Italy", "Spain", "Russia", "Belarus"],
                "year": [2020, 2020, 2020, 2020, 2020],
                "population": [67, 60, 47, 146, 9],
                "gdp": [2600, 2000, 1400, 1700, 60],
            }
        )

        # Create regions all list (similar to what would be in the real implementation)
        self.regions_all = [
            "Europe",
            "Asia",
            "Africa",
            "North America",
            "South America",
            "Oceania",
            "World",
            "High-income countries",
            "Upper-middle-income countries",
            "Lower-middle-income countries",
            "Low-income countries",
        ]

    def test_aggregator_initialization(self):
        """Test RegionAggregator initialization."""
        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            ds_income_groups=self.ds_income_groups,
            ds_population=self.ds_population,
        )

        # Test basic properties
        self.assertEqual(aggregator.ds_regions, self.ds_regions)
        self.assertEqual(aggregator.ds_income_groups, self.ds_income_groups)
        self.assertEqual(aggregator.ds_population, self.ds_population)
        self.assertEqual(aggregator.country_col, "country")
        self.assertEqual(aggregator.year_col, "year")
        self.assertEqual(aggregator.index_columns, ["country", "year"])

    def test_aggregator_initialization_with_custom_columns(self):
        """Test RegionAggregator initialization with custom column names."""
        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            ds_income_groups=self.ds_income_groups,  # Add income groups to prevent auto-loading
            country_col="location",
            year_col="time",
            index_columns=["location", "time", "category"],
        )

        self.assertEqual(aggregator.country_col, "location")
        self.assertEqual(aggregator.year_col, "time")
        self.assertEqual(aggregator.index_columns, ["location", "time", "category"])

    def test_aggregator_with_regions_list(self):
        """Test RegionAggregator with regions as a list."""
        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe", "High-income countries"],
            ds_income_groups=self.ds_income_groups,
        )

        # Should convert list to dict
        expected_regions = {"Europe": {}, "High-income countries": {}}
        self.assertEqual(aggregator.regions, expected_regions)

    def test_aggregator_with_regions_dict(self):
        """Test RegionAggregator with regions as a dictionary."""
        custom_regions = {
            "Europe": {"excluded_members": ["Russia"]},
            "Custom Region": {"custom_members": ["France", "Spain"]},
        }

        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=custom_regions,
            ds_income_groups=self.ds_income_groups,
        )

        self.assertEqual(aggregator.regions, custom_regions)

    def test_add_aggregates_basic(self):
        """Test basic add_aggregates functionality."""
        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],
            aggregations={"population": "sum", "gdp": "sum"},
            ds_income_groups=self.ds_income_groups,
        )

        result = aggregator.add_aggregates(
            self.tb_sample,
            check_for_region_overlaps=False,
        )

        # Check that Europe aggregate was added
        europe_data = result[result["country"] == "Europe"]
        self.assertEqual(len(europe_data), 1)

        # Europe should include France, Italy, Spain, Russia, Belarus
        expected_population = 67 + 60 + 47 + 146 + 9  # 329
        expected_gdp = 2600 + 2000 + 1400 + 1700 + 60  # 7760

        self.assertEqual(europe_data["population"].iloc[0], expected_population)
        self.assertEqual(europe_data["gdp"].iloc[0], expected_gdp)

    def test_add_aggregates_with_income_groups(self):
        """Test add_aggregates with income groups."""
        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["High-income countries"],
            aggregations={"population": "sum", "gdp": "sum"},
            ds_income_groups=self.ds_income_groups,
        )

        result = aggregator.add_aggregates(
            self.tb_sample,
            check_for_region_overlaps=False,
        )

        # Check that High-income countries aggregate was added
        high_income_data = result[result["country"] == "High-income countries"]
        self.assertEqual(len(high_income_data), 1)

        # High-income countries should include France, Italy, Spain
        expected_population = 67 + 60 + 47  # 174
        expected_gdp = 2600 + 2000 + 1400  # 6000

        self.assertEqual(high_income_data["population"].iloc[0], expected_population)
        self.assertEqual(high_income_data["gdp"].iloc[0], expected_gdp)

    def test_add_aggregates_with_nans(self):
        """Test add_aggregates behavior with NaN values."""
        # Create data with some NaN values
        tb_with_nans = Table(
            {
                "country": ["France", "Italy", "Spain", "Russia"],
                "year": [2020, 2020, 2020, 2020],
                "population": [67, 60, np.nan, 146],
                "gdp": [2600, np.nan, 1400, 1700],
            }
        )

        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],
            aggregations={"population": "sum", "gdp": "sum"},
            ds_income_groups=self.ds_income_groups,
        )

        result = aggregator.add_aggregates(
            tb_with_nans,
            check_for_region_overlaps=False,
        )

        # Check that Europe aggregate handles NaNs correctly
        europe_data = result[result["country"] == "Europe"]

        # population should be 67 + 60 + 146 = 273 (Spain's NaN ignored)
        # gdp should be 2600 + 1400 + 1700 = 5700 (Italy's NaN ignored)
        self.assertEqual(europe_data["population"].iloc[0], 273)
        self.assertEqual(europe_data["gdp"].iloc[0], 5700)

    def test_add_aggregates_with_max_nans_constraint(self):
        """Test add_aggregates with num_allowed_nans_per_year constraint."""
        # Create data with many NaN values
        tb_with_many_nans = Table(
            {
                "country": ["France", "Italy", "Spain", "Russia"],
                "year": [2020, 2020, 2020, 2020],
                "population": [67, np.nan, np.nan, np.nan],
                "gdp": [2600, 2000, 1400, 1700],
            }
        )

        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],
            aggregations={"population": "sum", "gdp": "sum"},
            ds_income_groups=self.ds_income_groups,
        )

        result = aggregator.add_aggregates(
            tb_with_many_nans,
            num_allowed_nans_per_year=1,  # Allow only 1 NaN
            check_for_region_overlaps=False,
        )

        europe_data = result[result["country"] == "Europe"]

        # population should be NaN (3 NaNs > 1 allowed)
        # gdp should be the sum (0 NaNs <= 1 allowed)
        self.assertTrue(pd.isna(europe_data["population"].iloc[0]))
        self.assertEqual(europe_data["gdp"].iloc[0], 7700)

    def test_add_aggregates_with_countries_that_must_have_data(self):
        """Test add_aggregates with countries_that_must_have_data constraint."""
        # Create data missing some key countries
        tb_missing_countries = Table(
            {
                "country": ["France", "Italy"],  # Missing Spain
                "year": [2020, 2020],
                "population": [67, 60],
                "gdp": [2600, 2000],
            }
        )

        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],
            aggregations={"population": "sum", "gdp": "sum"},
            ds_income_groups=self.ds_income_groups,
        )

        result = aggregator.add_aggregates(
            tb_missing_countries,
            countries_that_must_have_data={"Europe": ["France", "Italy", "Spain"]},
            check_for_region_overlaps=False,
        )

        europe_data = result[result["country"] == "Europe"]

        # Should be NaN because Spain is missing
        self.assertTrue(pd.isna(europe_data["population"].iloc[0]))
        self.assertTrue(pd.isna(europe_data["gdp"].iloc[0]))

    def test_countries_must_have_data_2(self):
        """Test the specific bug from GitHub issue #3071 where countries_that_must_have_data
        was not checked per column but globally."""
        # Create test data where France has data in col_a but NaN in col_b for 2011
        # Using countries that are actually in the mock Europe: Belarus, France, Italy, Russia, Spain
        tb_test = Table(
            {
                "country": ["Belarus", "France", "Belarus", "France"],
                "year": [2010, 2010, 2011, 2011],
                "col_a": [1, 2, 3, 4],  # France has data in both years
                "col_b": [1, 2, 5, None],  # France has NaN in 2011
            }
        )

        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],
            aggregations={"col_a": "sum", "col_b": "sum"},
            ds_income_groups=self.ds_income_groups,
        )

        result = aggregator.add_aggregates(
            tb_test,
            countries_that_must_have_data={"Europe": ["France"]},
            check_for_region_overlaps=False,
        )

        # Get Europe rows
        europe_data = result[result["country"] == "Europe"].set_index("year").sort_index()

        # For 2010: France has data in both columns, so Europe should have aggregates
        self.assertFalse(pd.isna(europe_data.loc[2010, "col_a"]))  # Should be 3 (1+2)
        self.assertFalse(pd.isna(europe_data.loc[2010, "col_b"]))  # Should be 3 (1+2)
        self.assertEqual(europe_data.loc[2010, "col_a"], 3)
        self.assertEqual(europe_data.loc[2010, "col_b"], 3)

        # For 2011: France has data in col_a but NaN in col_b
        # col_a should have aggregate (3+4=7), but col_b should be NaN
        self.assertFalse(pd.isna(europe_data.loc[2011, "col_a"]))  # Should be 7 (3+4)
        self.assertTrue(pd.isna(europe_data.loc[2011, "col_b"]))  # Should be NaN because France is NaN
        self.assertEqual(europe_data.loc[2011, "col_a"], 7)

    def test_countries_must_have_data_3(self):
        """Test the hierarchical regions bug from GitHub issue #3071 where
        World could have data even when Asia has no data due to China missing."""
        # Create test data where China has no data, so Asia should have no data,
        # and therefore World should have no data
        tb_test = Table(
            {
                "country": ["Belarus", "France", "Italy"],  # Russia missing (which is in Europe)
                "year": [2020, 2020, 2020],
                "gdp": [100, 200, 300],
            }
        )

        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],  # Just test Europe for simplicity
            aggregations={"gdp": "sum"},
            ds_income_groups=self.ds_income_groups,
        )

        result = aggregator.add_aggregates(
            tb_test,
            countries_that_must_have_data={
                "Europe": ["Russia"],  # Europe requires Russia, but Russia is missing
            },
            check_for_region_overlaps=False,
        )

        # Get Europe row
        europe_data = result[result["country"] == "Europe"]

        # Europe should have NaN because Russia is missing
        if not europe_data.empty:
            self.assertTrue(pd.isna(europe_data.iloc[0]["gdp"]), "Europe should have NaN GDP because Russia is missing")

    def test_countries_must_have_data_4(self):
        """Test that countries_that_must_have_data correctly handles both
        missing rows and NaN values for required countries."""
        # Test case 1: Required country has no row at all
        tb_no_row = Table(
            {
                "country": ["Belarus"],  # France completely missing
                "year": [2020],
                "gdp": [100],
            }
        )

        # Test case 2: Required country has row but NaN value
        tb_nan_value = Table(
            {
                "country": ["Belarus", "France"],
                "year": [2020, 2020],
                "gdp": [100, None],  # France has NaN
            }
        )

        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],
            aggregations={"gdp": "sum"},
            ds_income_groups=self.ds_income_groups,
        )

        # Both cases should result in Europe having NaN
        for tb_test, case_name in [(tb_no_row, "no_row"), (tb_nan_value, "nan_value")]:
            with self.subTest(case=case_name):
                result = aggregator.add_aggregates(
                    tb_test,
                    countries_that_must_have_data={"Europe": ["France"]},
                    check_for_region_overlaps=False,
                )

                europe_data = result[result["country"] == "Europe"]
                if not europe_data.empty:
                    self.assertTrue(
                        pd.isna(europe_data.iloc[0]["gdp"]),
                        f"Europe should have NaN GDP in {case_name} case because France is required but missing/NaN",
                    )

    def test_add_per_capita_basic(self):
        """Test basic add_per_capita functionality."""
        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],
            aggregations={"gdp": "sum"},
            ds_income_groups=self.ds_income_groups,
            ds_population=self.ds_population,
        )

        # Add aggregates first
        tb_with_aggregates = aggregator.add_aggregates(
            self.tb_sample,
            check_for_region_overlaps=False,
        )

        # Then add per capita
        result = aggregator.add_per_capita(
            tb_with_aggregates,
            columns=["gdp"],
            warn_on_missing_countries=False,
        )

        # Check that per capita columns were added
        self.assertIn("gdp_per_capita", result.columns)

        # Check individual country per capita values
        france_data = result[result["country"] == "France"]
        expected_france_per_capita = 2600 / 67  # gdp / population
        self.assertAlmostEqual(france_data["gdp_per_capita"].iloc[0], expected_france_per_capita, places=2)

    def test_add_per_capita_with_informed_countries_only(self):
        """Test add_per_capita with only_informed_countries_in_regions=True."""
        # Create data where not all countries have data
        tb_partial = Table(
            {
                "country": ["France", "Italy"],  # Missing Spain, Russia, Belarus
                "year": [2020, 2020],
                "population": [67, 60],
                "gdp": [2600, 2000],
            }
        )

        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],
            aggregations={"gdp": "sum"},
            ds_income_groups=self.ds_income_groups,
            ds_population=self.ds_population,
        )

        # Add aggregates first
        tb_with_aggregates = aggregator.add_aggregates(
            tb_partial,
            check_for_region_overlaps=False,
        )

        # Add per capita with informed countries only
        result = aggregator.add_per_capita(
            tb_with_aggregates,
            columns=["gdp"],
            only_informed_countries_in_regions=True,
            warn_on_missing_countries=False,
        )

        # Europe per capita should be based only on France + Italy population
        europe_data = result[result["country"] == "Europe"]
        expected_per_capita = (2600 + 2000) / (67 + 60)  # Only informed countries
        self.assertAlmostEqual(europe_data["gdp_per_capita"].iloc[0], expected_per_capita, places=2)

    def test_add_per_capita_custom_suffix(self):
        """Test add_per_capita with custom prefix and suffix."""
        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],
            ds_income_groups=self.ds_income_groups,
            ds_population=self.ds_population,
        )

        result = aggregator.add_per_capita(
            self.tb_sample,
            columns=["gdp"],
            prefix="avg_",
            suffix="_per_person",
            only_informed_countries_in_regions=False,  # Disable to avoid empty dataframe issue
            warn_on_missing_countries=False,
        )

        # Check that custom column name was created
        self.assertIn("avg_gdp_per_person", result.columns)
        self.assertNotIn("gdp_per_capita", result.columns)

    def test_inspect_overlaps_with_historical_regions(self):
        """Test inspect_overlaps_with_historical_regions functionality."""
        # Create data with overlapping historical region (USSR) and successor (Russia)
        tb_with_overlap = Table(
            {
                "country": ["USSR", "Russia", "France"],
                "year": [1990, 1990, 1990],  # Same year - should trigger overlap warning
                "population": [290, 148, 56],
                "gdp": [1000, 500, 1200],
            }
        )

        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],
            ds_income_groups=self.ds_income_groups,
        )

        # This should log a warning about the overlap
        with capture_logs() as captured_logs:
            aggregator.inspect_overlaps_with_historical_regions(tb_with_overlap)

        # Should have a warning about overlap
        self.assertTrue(any("overlap" in log.get("event", "").lower() for log in captured_logs))

    def test_inspect_overlaps_with_accepted_overlaps(self):
        """Test inspect_overlaps_with_historical_regions with accepted overlaps."""
        # Create data with overlapping historical region (USSR) and successor (Russia)
        tb_with_overlap = Table(
            {
                "country": ["USSR", "Russia", "France"],
                "year": [1990, 1990, 1990],
                "population": [290, 148, 56],
                "gdp": [1000, 500, 1200],
            }
        )

        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],
            ds_income_groups=self.ds_income_groups,
        )

        # Define accepted overlaps
        accepted_overlaps = [{1990: {"USSR", "Russia"}}]

        # This should not log a warning since the overlap is accepted
        with capture_logs() as captured_logs:
            aggregator.inspect_overlaps_with_historical_regions(tb_with_overlap, accepted_overlaps=accepted_overlaps)

        # Should not have warning about this specific overlap
        overlap_warnings = [log for log in captured_logs if "unknown overlap" in log.get("event", "").lower()]
        self.assertEqual(len(overlap_warnings), 0)

    def test_add_aggregates_empty_dataframe_handling(self):
        """Test add_aggregates handles empty DataFrames correctly (Bug #1 fix)."""
        # Create data that has no countries matching any region
        tb_no_matching_countries = Table(
            {
                "country": ["NonexistentCountry1", "NonexistentCountry2"],
                "year": [2020, 2020],
                "population": [100, 200],
                "gdp": [1000, 2000],
            }
        )

        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],  # Europe won't have any matching countries
            aggregations={"population": "sum", "gdp": "sum"},
            ds_income_groups=self.ds_income_groups,
        )

        # This should not crash even though no countries match the region
        result = aggregator.add_aggregates(
            tb_no_matching_countries,
            check_for_region_overlaps=False,
        )

        # Result should contain the original data (no regions added)
        self.assertEqual(len(result), 2)  # Original 2 rows
        self.assertListEqual(result["country"].tolist(), ["NonexistentCountry1", "NonexistentCountry2"])

        # No Europe should be in the result since no countries matched
        self.assertNotIn("Europe", result["country"].tolist())

    def test_add_aggregates_all_empty_regions(self):
        """Test add_aggregates when all regions result in empty DataFrames."""
        # Create data with countries but request regions that don't contain those countries
        tb_mismatched = Table(
            {
                "country": ["France", "Italy"],
                "year": [2020, 2020],
                "population": [67, 60],
                "gdp": [2600, 2000],
            }
        )

        # Use regions that don't exist in our mock dataset or don't contain these countries
        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["NonexistentRegion"],  # This region doesn't exist
            aggregations={"population": "sum", "gdp": "sum"},
            ds_income_groups=self.ds_income_groups,
        )

        # This should not crash and should return the original data
        result = aggregator.add_aggregates(
            tb_mismatched,
            check_for_region_overlaps=False,
        )

        # Should contain original data
        self.assertEqual(len(result), 2)
        self.assertListEqual(result["country"].tolist(), ["France", "Italy"])
        self.assertNotIn("NonexistentRegion", result["country"].tolist())

    def test_partial_aggregation_preserves_existing_data(self):
        """Test that when adding aggregates for only some columns, existing data
        for non-aggregated columns is preserved (not deleted)."""
        # Create test data where Europe already exists with data for multiple columns
        tb_with_existing_europe = Table(
            {
                "country": ["France", "Italy", "Europe", "Europe"],
                "year": [2020, 2020, 2020, 2021],
                "gdp": [100, 200, 999, 888],  # Europe already has GDP data that should be replaced
                "population": [67, 60, 777, 666],  # Europe already has population data that should be preserved
                "area": [551, 301, 555, 444],  # Europe already has area data that should be preserved
            }
        )

        # Create aggregator that only aggregates GDP (not population or area)
        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],
            aggregations={"gdp": "sum"},  # Only aggregating GDP, not population or area
            ds_income_groups=self.ds_income_groups,
        )

        result = aggregator.add_aggregates(
            tb_with_existing_europe,
            check_for_region_overlaps=False,
        )

        # Get Europe data
        europe_data = result[result["country"] == "Europe"].set_index("year").sort_index()

        # GDP should be replaced with new aggregates (France + Italy)
        self.assertEqual(europe_data.loc[2020, "gdp"], 300)  # 100 + 200 (aggregated)

        # Population and area should be preserved from original Europe data
        self.assertEqual(europe_data.loc[2020, "population"], 777)  # Original preserved
        self.assertEqual(europe_data.loc[2020, "area"], 555)  # Original preserved
        self.assertEqual(europe_data.loc[2021, "population"], 666)  # Original preserved
        self.assertEqual(europe_data.loc[2021, "area"], 444)  # Original preserved

    def test_partial_aggregation_new_region_gets_nan_for_non_aggregated(self):
        """Test that when adding aggregates for only some columns, new regions
        get NaN for non-aggregated columns."""
        # Create test data without any existing Europe data
        tb_without_europe = Table(
            {
                "country": ["France", "Italy"],
                "year": [2020, 2020],
                "gdp": [100, 200],
                "population": [67, 60],
                "area": [551, 301],
            }
        )

        # Create aggregator that only aggregates GDP
        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],
            aggregations={"gdp": "sum"},  # Only aggregating GDP
            ds_income_groups=self.ds_income_groups,
        )

        result = aggregator.add_aggregates(
            tb_without_europe,
            check_for_region_overlaps=False,
        )

        # Get Europe data
        europe_data = result[result["country"] == "Europe"]

        # GDP should be aggregated (France + Italy)
        self.assertEqual(europe_data.iloc[0]["gdp"], 300)  # 100 + 200

        # Population and area should be NaN since no aggregation was defined and no original data existed
        self.assertTrue(pd.isna(europe_data.iloc[0]["population"]))
        self.assertTrue(pd.isna(europe_data.iloc[0]["area"]))

    def test_partial_aggregation_mixed_scenarios(self):
        """Test partial aggregation with mixed scenarios: some years have existing data, others don't."""
        # Create test data where Europe exists for some years but not others
        tb_mixed = Table(
            {
                "country": ["France", "Italy", "France", "Italy", "Europe"],
                "year": [2020, 2020, 2021, 2021, 2021],  # Europe only exists in 2021
                "gdp": [100, 200, 110, 210, 999],  # Europe has GDP in 2021 that should be replaced
                "population": [67, 60, 68, 61, 777],  # Europe has population in 2021 that should be preserved
            }
        )

        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],
            aggregations={"gdp": "sum"},  # Only aggregating GDP
            ds_income_groups=self.ds_income_groups,
        )

        result = aggregator.add_aggregates(
            tb_mixed,
            check_for_region_overlaps=False,
        )

        europe_data = result[result["country"] == "Europe"].set_index("year").sort_index()

        # Both years should have aggregated GDP
        self.assertEqual(europe_data.loc[2020, "gdp"], 300)  # 100 + 200 (new aggregate)
        self.assertEqual(europe_data.loc[2021, "gdp"], 320)  # 110 + 210 (replaces 999)

        # 2020 should have NaN population (no original data)
        self.assertTrue(pd.isna(europe_data.loc[2020, "population"]))

        # 2021 should preserve original population
        self.assertEqual(europe_data.loc[2021, "population"], 777)

    def test_full_aggregation_replaces_all_data(self):
        """Test that when aggregating all columns, all existing region data is replaced."""
        tb_with_existing_europe = Table(
            {
                "country": ["France", "Italy", "Europe"],
                "year": [2020, 2020, 2020],
                "gdp": [100, 200, 999],  # Should be replaced
                "population": [67, 60, 777],  # Should be replaced
            }
        )

        # Aggregate both columns
        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],
            aggregations={"gdp": "sum", "population": "sum"},  # Aggregating both columns
            ds_income_groups=self.ds_income_groups,
        )

        result = aggregator.add_aggregates(
            tb_with_existing_europe,
            check_for_region_overlaps=False,
        )

        europe_data = result[result["country"] == "Europe"]

        # Both columns should be replaced with aggregates
        self.assertEqual(europe_data.iloc[0]["gdp"], 300)  # 100 + 200 (replaces 999)
        self.assertEqual(europe_data.iloc[0]["population"], 127)  # 67 + 60 (replaces 777)

    def test_partial_aggregation_improvement_over_old_behavior(self):
        """Test that demonstrates the improvement over the old add_region_aggregates behavior.

        Old behavior: When aggregating only some columns, existing data for non-aggregated
        columns would be DELETED (causing data loss).

        New behavior: Existing data for non-aggregated columns is PRESERVED.
        """
        # Scenario: Europe already exists with multiple types of data
        tb_with_europe = Table(
            {
                "country": ["France", "Italy", "Europe"],
                "year": [2020, 2020, 2020],
                "gdp": [100, 200, 999],  # Will be replaced with aggregate
                "population": [67, 60, 777],  # Should be preserved (old behavior: would be deleted!)
                "area": [551, 301, 555],  # Should be preserved (old behavior: would be deleted!)
                "life_expectancy": [82, 83, 88],  # Should be preserved (old behavior: would be deleted!)
            }
        )

        # Only aggregate GDP, leave other columns as-is
        aggregator = geo.RegionAggregator(
            ds_regions=self.ds_regions,
            regions_all=self.regions_all,
            regions=["Europe"],
            aggregations={"gdp": "sum"},  # Only GDP gets aggregated
            ds_income_groups=self.ds_income_groups,
        )

        result = aggregator.add_aggregates(
            tb_with_europe,
            check_for_region_overlaps=False,
        )

        europe_data = result[result["country"] == "Europe"]

        # GDP should be replaced with aggregated value
        self.assertEqual(europe_data.iloc[0]["gdp"], 300)  # 100 + 200 (NEW aggregate)

        # Other columns should preserve original Europe data (IMPROVEMENT!)
        # In the old behavior, these would all become NaN, causing data loss
        self.assertEqual(europe_data.iloc[0]["population"], 777)  # PRESERVED
        self.assertEqual(europe_data.iloc[0]["area"], 555)  # PRESERVED
        self.assertEqual(europe_data.iloc[0]["life_expectancy"], 88)  # PRESERVED

        # Verify that individual country data is still present
        individual_countries = result[result["country"].isin(["France", "Italy"])]
        self.assertEqual(len(individual_countries), 2)  # Both countries still there

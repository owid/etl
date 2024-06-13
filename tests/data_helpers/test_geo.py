"""Test functions in etl.data_helpers.geo module.

"""

import json
import unittest
import warnings
from unittest.mock import mock_open, patch

import numpy as np
import pandas as pd
from owid.catalog import Table
from owid.datautils import dataframes
from pytest import warns
from structlog.testing import capture_logs

from etl.data_helpers import geo

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


@patch.object(geo, "_load_population", mock_population_load)
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
        assert geo.add_population_to_dataframe(df=df_in).equals(df_out)

    def test_countries_and_years_in_population_just_one(self):
        df_in = pd.DataFrame({"country": ["Country 2", "Country 2"], "year": [2020, 2019]})
        df_out = pd.DataFrame(
            {
                "country": ["Country 2", "Country 2"],
                "year": [2020, 2019],
                "population": [40, 30],
            }
        )
        assert geo.add_population_to_dataframe(df=df_in).equals(df_out)

    def test_one_country_in_and_another_not_in_population(self):
        df_in = pd.DataFrame({"country": ["Country 1", "Country 3"], "year": [2020, 2021]})
        df_out = pd.DataFrame(
            {
                "country": ["Country 1", "Country 3"],
                "year": [2020, 2021],
                "population": [10, np.nan],
            }
        )
        assert geo.add_population_to_dataframe(df=df_in).equals(df_out)

    def test_no_countries_in_population(self):
        df_in = pd.DataFrame({"country": ["Country_04", "Country_04"], "year": [2000, 2000]})
        df_out = pd.DataFrame(
            {
                "country": ["Country_04", "Country_04"],
                "year": [2000, 2000],
                "population": [np.nan, np.nan],
            }
        )
        assert geo.add_population_to_dataframe(df=df_in, warn_on_missing_countries=False).equals(df_out)

    def test_countries_in_population_but_not_for_given_years(self):
        df_in = pd.DataFrame({"country": ["Country 2", "Country 1"], "year": [2000, 2000]})
        df_out = pd.DataFrame(
            {
                "country": ["Country 2", "Country 1"],
                "year": [2000, 2000],
                "population": [np.nan, np.nan],
            }
        )
        assert geo.add_population_to_dataframe(df=df_in).equals(df_out)

    def test_countries_in_population_but_a_year_in_and_another_not_in_population(self):
        df_in = pd.DataFrame({"country": ["Country 2", "Country 1"], "year": [2019, 2000]})
        df_out = pd.DataFrame(
            {
                "country": ["Country 2", "Country 1"],
                "year": [2019, 2000],
                "population": [30, np.nan],
            }
        )
        assert geo.add_population_to_dataframe(df=df_in).equals(df_out)

    def test_change_country_and_year_column_names(self):
        df_in = pd.DataFrame({"Country": ["Country 2", "Country 1"], "Year": [2019, 2021]})
        df_out = pd.DataFrame(
            {
                "Country": ["Country 2", "Country 1"],
                "Year": [2019, 2021],
                "population": [30, 20],
            }
        )
        assert geo.add_population_to_dataframe(df=df_in, country_col="Country", year_col="Year").equals(df_out)

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
            geo.add_population_to_dataframe(df=df_in, warn_on_missing_countries=True).equals(df_out)


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
        df_out = pd.DataFrame({"country": [np.nan, np.nan], "some_variable": [1, 2]})
        df_out["country"] = df_out["country"].astype(object)
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
        assert dataframes.are_equal(
            df1=df_out,
            df2=geo.harmonize_countries(df=df_in, countries_file="MOCK_COUNTRIES_FILE", warn_on_unused_countries=False),
        )[0]

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
                "members": ['["BLR", "FRA", "ITA", "RUS", "ESP", "OWID_USS"]', "[]", "[]", "[]", "[]", "[]", "[]"],
                "successors": ["[]", "[]", "[]", "[]", "[]", "[]", '["BLR", "RUS"]'],
            }
        ).set_index("code")
        return mock_tb_regions


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


ds_regions = MockRegionsDataset()
ds_income_groups = MockIncomeGroupsDataset()


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

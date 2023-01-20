"""Test functions in etl.data_helpers.geo module.

"""

import json
import unittest
import warnings
from unittest.mock import mock_open, patch

import numpy as np
import pandas as pd
from owid.datautils import dataframes
from pytest import warns

from etl.data_helpers import geo

mock_countries = {
    "country_02": "Country 2",
    "country_03": "Country 3",
}

mock_excluded_countries = [
    "country_05",
    "country_06",
]

mock_population = pd.DataFrame(
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
            warnings.simplefilter("error")
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
                "var_02": [10.0, 20.0, 30.0, 40.0, 60.0, 50.0, 40.0],
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
                "var_02": [10.0, 20.0, 30.0, 40.0, 60.0, 50.0, 40.0],
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
                "var_02": [10.0, 20.0, 30.0, 40.0, 60.0, 10.0, 20.0, 50.0],
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
                "var_02": [10.0, 20.0, 30.0, 40.0, 60.0, 10.0 + 30.0, 20.0],
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
                "var_02": [10.0, 20.0, 30.0, 40.0, 60.0, 10.0, 20.0, 50.0],
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
                "var_02": [10.0, 20.0, 30.0, 40.0, 60.0, 40.0, 20.0, 50.0],
            }
        )
        assert dataframes.are_equal(df1=df, df2=df_out)[0]

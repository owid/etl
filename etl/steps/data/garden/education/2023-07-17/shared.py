"""Custom add regional aggregates function for calculating population weighted averages"""

from typing import Any, Dict, List, Optional, Union, cast

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.datautils.dataframes import groupby_agg

from etl.data_helpers import geo

# When creating region aggregates for a certain variable in a certain year, some mandatory countries must be
# informed, otherwise the aggregate will be nan (since we consider that there is not enough information).
# A country will be considered mandatory if they exceed this minimum fraction of the total population of the region.
MIN_FRAC_INDIVIDUAL_POPULATION = 0.0
# A country will be considered mandatory if the sum of the population of all countries (sorted by decreasing
# population until reaching this country) exceeds the following fraction of the total population of the region.
MIN_FRAC_CUMULATIVE_POPULATION = 0.7
# Reference year to build the list of mandatory countries.
REFERENCE_YEAR = 2018
# Maximum fraction of nans allowed per year when doing aggregations (None to allow any fraction of nans).
FRAC_ALLOWED_NANS_PER_YEAR = 0.2
# Maximum number of nans allowed per year when doing aggregations (None to allow any number of nans).
NUM_ALLOWED_NANS_PER_YEAR = None


def add_region_aggregates_education(
    df: pd.DataFrame,
    region: str,
    countries_in_region: Optional[List[str]] = None,
    countries_that_must_have_data: Optional[List[str]] = None,
    num_allowed_nans_per_year: Union[int, None] = NUM_ALLOWED_NANS_PER_YEAR,
    frac_allowed_nans_per_year: Union[float, None] = FRAC_ALLOWED_NANS_PER_YEAR,
    country_col: str = "country",
    year_col: str = "year",
    aggregations: Optional[Dict[str, Any]] = None,
    keep_original_region_with_suffix: Optional[str] = None,
    population: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Add data for regions (e.g. income groups or continents) to a dataset.

    If data for a region already exists in the dataset, it will be replaced.

    When adding up the contribution from different countries (e.g. Spain, France, etc.) of a region (e.g. Europe), we
    want to avoid two problems:
    * Generating a series of nan, because one small country (with a negligible contribution) has nans.
    * Generating a series that underestimates the real one, because of treating missing values as zeros.

    To avoid these problems, we first define a list of "big countries" that must be present in the data, in order to
    safely do the aggregation. If any of these countries is not present for a particular variable and year, the
    aggregation will be nan for that variable and year. Otherwise, if all big countries are present, any other missing
    country will be assumed to have zero contribution to the variable.
    For example, when aggregating the electricity demand of North America, United States and Mexico cannot be missing,
    because otherwise the aggregation would significantly underestimate the true electricity demand of North America.

    Additionally, the aggregation of a particular variable for a particular year cannot have too many nans. If the
    number of nans exceeds num_allowed_nans_per_year, or if the fraction of nans exceeds frac_allowed_nans_per_year, the
    aggregation for that variable and year will be nan.

    Parameters
    ----------
    df : pd.Dataframe
        Original dataset, which may contain data for that region (in which case, it will be replaced by the ).
    region : str
        Region to add.
    countries_in_region : list or None
        List of countries that are members of this region. None to load them from countries-regions dataset.
    countries_that_must_have_data : list or None
        List of countries that must have data for a particular variable and year, otherwise the region will have nan for
        that particular variable and year. See function list_countries_in_region_that_must_have_data for more
        details.
    num_allowed_nans_per_year : int or None
        Maximum number of nans that can be present in a particular variable and year. If exceeded, the aggregation will
        be nan.
    frac_allowed_nans_per_year : float or None
        Maximum fraction of nans that can be present in a particular variable and year. If exceeded, the aggregation
        will be nan.
    country_col : str
        Name of country column.
    year_col : str
        Name of year column.
    aggregations : dict or None
        Aggregations to execute for each variable. If None, the contribution to each variable from each country in the
        region will be summed. Otherwise, only the variables indicated in the dictionary will be affected. All remaining
        variables will be nan.
    keep_original_region_with_suffix : str or None
        If None, original data for region will be replaced by aggregate data constructed by this function. If not None,
        original data for region will be kept, with the same name, but having suffix keep_original_region_with_suffix
        added to its name.
    population : pd.DataFrame or None
        Population dataset, or None, to load it from owid catalog.

    Returns
    -------
    df_updated : pd.DataFrame
        Original dataset after adding (or replacing) data for selected region.

    """
    if countries_in_region is None:
        # List countries in the region.
        countries_in_region = geo.list_countries_in_region(
            region=region,
        )

    if countries_that_must_have_data is None:
        # List countries that should present in the data (since they are expected to contribute the most).
        countries_that_must_have_data = geo.list_countries_in_region_that_must_have_data(
            region=region,
            population=population,
        )

    # If aggregations are not defined for each variable, assume 'sum'.
    fixed_columns = [country_col, year_col]
    if aggregations is None:
        aggregations = {variable: "sum" for variable in df.columns if variable not in fixed_columns}
    variables = list(aggregations)

    # Select data for countries in the region.
    df_countries = df[df[country_col].isin(countries_in_region)]
    df_countries = geo.add_population_to_dataframe(df_countries)

    weights = df_countries["population"]

    # Definte aggregations for each variable.
    aggs = {
        country_col: lambda x: set(countries_that_must_have_data).issubset(set(list(x))),
    }
    for variable in variables:
        # If aggreggate is mean then do weighted average using population data, replacing `aggregations[variable]` with a lambda function

        if weights is not None:
            assert aggregations[variable] == "mean", "Weights only work mean aggregation"

            # Define your weighted mean function
            def weighted_mean(x, w):
                values = np.ma.masked_invalid(x.astype("float64"))
                weights = np.ma.masked_invalid(w.astype("float64"))
                return np.ma.average(values, weights=weights)

            # Create a closure to define variable_agg with specific weights
            def make_weighted_mean(weights):
                def variable_agg(x):
                    return weighted_mean(x, weights.loc[x.index])

                return variable_agg

            variable_agg = make_weighted_mean(weights)

        else:
            variable_agg = aggregations[variable]

        aggs[variable] = variable_agg

    df_region = groupby_agg(
        df=df_countries,
        groupby_columns=year_col,
        aggregations=aggs,
        num_allowed_nans=num_allowed_nans_per_year,
        frac_allowed_nans=frac_allowed_nans_per_year,
    ).reset_index()

    # Make nan all aggregates if the most contributing countries were not present.
    df_region.loc[~df_region[country_col], variables] = np.nan
    # Replace the column that was used to check if most contributing countries were present by the region's name.
    df_region[country_col] = region

    if isinstance(keep_original_region_with_suffix, str):
        # Keep rows in the original dataframe containing rows for region (adding a suffix to the region name), and then
        # append new rows for region.
        rows_original_region = df[country_col] == region
        df_original_region = df[rows_original_region].reset_index(drop=True)
        # Append suffix at the end of the name of the original region.
        df_original_region[country_col] = region + cast(str, keep_original_region_with_suffix)
        df_updated = pr.concat(
            [df[~rows_original_region], df_original_region, df_region],
            ignore_index=True,
        )
    else:
        # Remove rows in the original dataframe containing rows for region, and append new rows for region.
        df_updated = pr.concat([df[~(df[country_col] == region)], df_region], ignore_index=True)

    # Sort conveniently.
    df_updated = df_updated.sort_values([country_col, year_col]).reset_index(drop=True)

    return df_updated

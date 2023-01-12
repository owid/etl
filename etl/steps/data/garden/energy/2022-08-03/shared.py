from pathlib import Path
from typing import Dict, List, Union, cast

import pandas as pd
from owid import catalog

from etl.data_helpers import geo
from etl.paths import DATA_DIR

CURRENT_DIR = Path(__file__).parent
VERSION = CURRENT_DIR.name


HISTORIC_TO_CURRENT_REGION: Dict[str, Dict[str, Union[str, List[str]]]] = {
    "Netherlands Antilles": {
        "continent": "North America",
        "income_group": "High-income countries",
        "members": [
            # North America - High-income countries.
            "Aruba",
            "Curacao",
            "Sint Maarten (Dutch part)",
        ],
    },
    "USSR": {
        "continent": "Europe",
        "income_group": "Upper-middle-income countries",
        "members": [
            # Europe - High-income countries.
            "Lithuania",
            "Estonia",
            "Latvia",
            # Europe - Upper-middle-income countries.
            "Moldova",
            "Belarus",
            "Russia",
            # Europe - Lower-middle-income countries.
            "Ukraine",
            # Asia - Upper-middle-income countries.
            "Georgia",
            "Armenia",
            "Azerbaijan",
            "Turkmenistan",
            "Kazakhstan",
            # Asia - Lower-middle-income countries.
            "Kyrgyzstan",
            "Uzbekistan",
            "Tajikistan",
        ],
    },
}


def combine_two_overlapping_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, index_columns: List[str]) -> pd.DataFrame:
    """Combine two dataframes that may have identical columns, prioritising the first one.

    Both dataframes must have a dummy index (if not, use reset_index() on both of them).

    Suppose you have two dataframes, df1 and df2, both having columns "col_a" and "col_b", and we want to create a
    combined dataframe with the union of rows and columns, and, on the overlapping elements, prioritise df1 values.
    To do this, you could:
    * Merge the dataframes. But then the result would have columns "col_a_x", "col_a_y", "col_b_x", and "col_b_y".
    * Concatenate them and then drop duplicates (for example keeping the last repetition). This work, but, if df1 has
      nans then we would keep those nans.
    To solve these problems, this function will not create new columns, and will prioritise df1 **only if it has data**,
    and otherwise use values from df2.

    Parameters
    ----------
    df1 : pd.DataFrame
        First dataframe (the one that has priority).
    df2 : pd.DataFrame
        Second dataframe.
    index_columns : list
        Columns (that must be present in both dataframes) that should be treated as index.

    Returns
    -------
    combined : pd.DataFrame
        Combination of the two dataframes.

    """
    # Find columns of data (those that are not index columns).
    data_columns = [col for col in (set(df1.columns) | set(df2.columns)) if col not in index_columns]

    # Go column by column, concatenate, remove nans, and then keep df1 version on duplicated rows.
    # Note: There may be a faster, simpler way to achieve this.
    combined = pd.DataFrame({column: [] for column in index_columns})
    for variable in data_columns:
        _df1 = pd.DataFrame()
        _df2 = pd.DataFrame()
        if variable in df1.columns:
            _df1 = df1[index_columns + [variable]].dropna(subset=variable)
        if variable in df2.columns:
            _df2 = df2[index_columns + [variable]].dropna(subset=variable)
        _combined = pd.concat([_df1, _df2], ignore_index=True)
        # On rows where both datasets overlap, give priority to df1.
        _combined = _combined.drop_duplicates(subset=index_columns, keep="first")
        # Add the current variable to the combined dataframe.
        combined = pd.merge(combined, _combined, on=index_columns, how="outer")

    assert len([column for column in combined.columns if column.endswith("_x")]) == 0, "There are repeated columns."

    return combined


def load_population() -> pd.DataFrame:
    """Load OWID population dataset, and add historical regions to it.

    Returns
    -------
    population : pd.DataFrame
        Population dataset.

    """
    # Load population dataset.
    population = catalog.Dataset(DATA_DIR / "garden/owid/latest/key_indicators/")["population"].reset_index()[
        ["country", "year", "population"]
    ]

    # Add data for historical regions (if not in population) by adding the population of its current successors.
    countries_with_population = population["country"].unique()
    missing_countries = [country for country in HISTORIC_TO_CURRENT_REGION if country not in countries_with_population]
    for country in missing_countries:
        members = HISTORIC_TO_CURRENT_REGION[country]["members"]
        _population = (
            population[population["country"].isin(members)]
            .groupby("year")
            .agg({"population": "sum", "country": "nunique"})
            .reset_index()
        )
        # Select only years for which we have data for all member countries.
        _population = _population[_population["country"] == len(members)].reset_index(drop=True)
        _population["country"] = country
        population = pd.concat([population, _population], ignore_index=True).reset_index(drop=True)

    error = "Duplicate country-years found in population. Check if historical regions changed."
    assert population[population.duplicated(subset=["country", "year"])].empty, error

    return cast(pd.DataFrame, population)


def add_population(
    df: pd.DataFrame,
    country_col: str = "country",
    year_col: str = "year",
    population_col: str = "population",
    warn_on_missing_countries: bool = True,
    show_full_warning: bool = True,
) -> pd.DataFrame:
    """Add a column of OWID population to the countries in the data, including population of historical regions.

    This function has been adapted from datautils.geo, because population currently does not include historic regions.
    We include them in this function.

    Parameters
    ----------
    df : pd.DataFrame
        Data without a column for population (after harmonizing elements, items and country names).
    country_col : str
        Name of country column in data.
    year_col : str
        Name of year column in data.
    population_col : str
        Name for new population column in data.
    warn_on_missing_countries : bool
        True to warn if population is not found for any of the countries in the data.
    show_full_warning : bool
        True to show affected countries if the previous warning is raised.

    Returns
    -------
    df_with_population : pd.DataFrame
        Data after adding a column for population for all countries in the data.

    """

    # Load population dataset.
    population = load_population().rename(
        columns={
            "country": country_col,
            "year": year_col,
            "population": population_col,
        }
    )[[country_col, year_col, population_col]]

    # Check if there is any missing country.
    missing_countries = set(df[country_col]) - set(population[country_col])
    if len(missing_countries) > 0:
        if warn_on_missing_countries:
            geo.warn_on_list_of_entities(
                list_of_entities=missing_countries,
                warning_message=(
                    f"{len(missing_countries)} countries not found in population"
                    " dataset. They will remain in the dataset, but have nan"
                    " population."
                ),
                show_list=show_full_warning,
            )

    # Add population to original dataframe.
    df_with_population = pd.merge(df, population, on=[country_col, year_col], how="left")

    return df_with_population

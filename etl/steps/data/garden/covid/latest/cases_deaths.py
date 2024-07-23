"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# add here country-dates where data should be set to NaN since the date specified
LARGE_DATA_CORRECTIONS_SINCE = [
    # ("United States", "2023-05-21", "deaths"),
    ("United States", "2023-05-21", "cases"),
    ("Spain", "2023-07-10", "deaths"),
    ("Spain", "2023-07-10", "cases"),
    ("Germany", "2023-07-10", "deaths"),
    ("Germany", "2023-07-10", "cases"),
    ("France", "2023-07-02", "deaths"),
    ("France", "2023-07-02", "cases"),
]
# add here country-dates where data should be set to NaN
LARGE_DATA_CORRECTIONS = [
    # ("Australia", "2022-04-01", "deaths"),
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("cases_deaths")

    # Read table from meadow dataset.
    tb = ds_meadow.read_table("cases_deaths")

    #
    # Process data.
    #
    paths.log.info("cleaning data")
    tb = clean_table(tb)

    # Country name harmonization
    paths.log.info("harmonizing country names")
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Aggregate international entities
    paths.log.info("aggregating international entities")
    tb = aggregate_international(tb)

    # HOTFIX: Data is only available every 7 days. Fill in the gaps with zeroes
    tb = fill_date_gaps(tb)

    # Main processing
    tb["date"] = pd.to_datetime(tb["date"], format="%Y-%m-%d")

    # Sort rows by country and date
    tb = tb.sort_values(["country", "date"])

    # Format
    tb = tb.format(["country", "date_reported"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def clean_table(tb: Table) -> Table:
    """Clean table.

    - Rename columns
    - Keep relevant columns
    - Sanity checks
    """
    # Rename and keep relevant columns
    column_renaming = {
        "country": "country",
        "date_reported": "date",
        "new_cases": "new_cases",
        "cumulative_cases": "total_cases",
        "new_deaths": "new_deaths",
        "cumulative_deaths": "total_deaths",
    }
    # Rename columns
    tb = tb.rename(columns=column_renaming)
    # Sort columns and rows
    tb = tb.loc[:, column_renaming.values()]

    # HOTFIX: remove countries with name set to NaN
    tb = tb.loc[~tb["country"].isna()]
    # Remove invalid locations
    # tb = tb.loc[~tb["country"].isin(["Icvanuatu", "Ickiribati"])]

    # Sanity checks
    assert (tb["total_deaths"] >= 0).all(), "Negative total deaths"
    assert (tb["total_cases"] >= 0).all(), "Negative total cases"

    return tb


def aggregate_international(tb: Table) -> Table:
    """Aggregate all 'International' entities.

    Multiple entities are mapped to 'International'. Their values should be aggregated.
    """
    # Sanity check
    x = tb.groupby(["country", "date"]).size()
    countries_duplicate = x[x > 1].index
    countries_duplicate = set(i[0] for i in countries_duplicate)
    assert countries_duplicate == {"International"}, "There are unexpected duplicates!"
    # Aggregate
    tb = tb.groupby(["country", "date"]).sum(min_count=1).reset_index()  # type: ignore
    return tb


def fill_date_gaps(tb: Table) -> Table:
    """Ensure dataframe has all dates.

    Apparently, in the past the input data had all the dates from start to end.

    Early in 2024 this stopped to be like this, maybe due to a change in how the data is reported by the WHO. Hence, we need to make sure that there are no gaps!
    Source of change might be this: https://github.com/owid/covid-19-data/commit/ed73e7113344caffc9e445946979e1964720348b#diff-cb6c8f3daa43ff50c0cac819d63ce03bedfd4c7cf98ace02cad543a485c9513e

    We do this by:
        - Reindexing the dataframe to have all dates for all locations.
        - Filling in NaNs with zeroes, for daily indicators.
        - Filling in NaNs with the last non-NaN value, for cumulative indicators (forward filling).
    """
    # Ensure date is of type date
    tb["date"] = pd.to_datetime(tb["date"], format="%Y-%m-%d").astype("datetime64[ns]")

    # Get set of locations
    countries = set(tb["country"])
    # Create index based on all locations and all dates
    complete_dates = pd.date_range(tb["date"].min(), tb["date"].max())

    # Reindex
    tb = tb.set_index(["country", "date"])
    new_index = pd.MultiIndex.from_product([countries, complete_dates], names=["country", "date"])
    tb = tb.reindex(new_index).sort_index().reset_index()

    # Fill in NaNs
    tb[["new_cases", "new_deaths"]] = tb[["new_cases", "new_deaths"]].fillna(0)
    tb[["total_cases", "total_deaths"]] = tb.groupby("country")[["total_cases", "total_deaths"]].fillna(method="ffill")  # type: ignore

    return tb


def discard_rows(tb: Table):
    print("Discarding rows…")
    # For all rows where new_cases or new_deaths is negative, we keep the cumulative value but set
    # the daily change to NA. This also sets the 7-day rolling average to NA for the next 7 days.
    tb.loc[tb["new_cases"] < 0, "new_cases"] = np.nan
    tb.loc[tb["new_deaths"] < 0, "new_deaths"] = np.nan

    # Custom data corrections
    for ldc in LARGE_DATA_CORRECTIONS:
        tb.loc[(tb["country"] == ldc[0]) & (tb["date"].astype(str) == ldc[1]), f"new_{ldc[2]}"] = np.nan

    for ldc in LARGE_DATA_CORRECTIONS_SINCE:
        tb.loc[(tb["country"] == ldc[0]) & (tb["date"].astype(str) >= ldc[1]), f"new_{ldc[2]}"] = np.nan

    # If the last known value is above 1000 cases or 100 deaths but the latest reported value is 0
    # then set that value to NA in case it's a temporary reporting error. (Up to 7 days in the past)
    tb = tb.sort_values(["country", "date"])
    tb = tb.groupby("country").apply(hide_recent_zeros)  # type: ignore

    return tb


def hide_recent_zeros(tb: pd.DataFrame) -> pd.DataFrame:
    last_reported_date = tb["date"].max()

    last_positive_cases_date = tb.loc[tb["new_cases"] > 0, "date"].max()
    if pd.isnull(last_positive_cases_date):
        return tb
    if last_positive_cases_date != last_reported_date:
        last_known_cases = tb.loc[tb["date"] == last_positive_cases_date, "new_cases"].item()
        if last_known_cases >= 100 and (last_reported_date - last_positive_cases_date).days < 7:
            tb.loc[tb["date"] > last_positive_cases_date, "new_cases"] = np.nan

    last_positive_deaths_date = tb.loc[tb["new_deaths"] > 0, "date"].max()
    if pd.isnull(last_positive_deaths_date):
        return tb
    if last_positive_deaths_date != last_reported_date:
        last_known_deaths = tb.loc[tb["date"] == last_positive_deaths_date, "new_deaths"].item()
        if last_known_deaths >= 10 and (last_reported_date - last_positive_deaths_date).days < 7:
            tb.loc[tb["date"] > last_positive_deaths_date, "new_deaths"] = np.nan

    return tb

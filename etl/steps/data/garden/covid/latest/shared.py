import datetime as dt
from typing import Optional, Set, cast

import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset


def run(dest_dir: str, paths: PathFinder) -> None:
    #
    # Load data from github.
    #
    url = paths.load_etag_url()

    df = pd.read_csv(url).rename(columns={"Year": "year", "Country": "country"})

    # Harmonize country names.
    df.country = df.country.replace(
        {
            "Faeroe Islands": "Faroe Islands",
            "Timor": "East Timor",
        }
    )

    df = df.set_index(["year", "country"]).dropna(axis=0, how="all")

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = create_dataset(dest_dir, tables=[tb])

    ds_garden.metadata.sources[0].date_accessed = str(dt.date.today())

    # Save changes in the new garden dataset.
    ds_garden.save()


def fill_date_gaps(tb: Table) -> Table:
    """Ensure dataframe has all dates.

    Apparently, in the past the input data had all the dates from start to end.

    Early in 2024 this stopped to be like this, maybe due to a change in how the data is reported by the WHO. Hence, we need to make sure that there are no gaps!
    Source of change might be this: https://github.com/owid/covid-19-data/commit/ed73e7113344caffc9e445946979e1964720348b#diff-cb6c8f3daa43ff50c0cac819d63ce03bedfd4c7cf98ace02cad543a485c9513e

    We do this by:
        - Reindexing the dataframe to have all dates for all locations.
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

    return tb


def make_table_population_daily(ds_population: Dataset, year_min: int, year_max: int) -> Table:
    """Create table with daily population.

    Uses linear interpolation.
    """
    # Load population table
    population = ds_population.read_table("population")
    # Filter only years of interest
    population = population[(population["year"] >= year_min) & (population["year"] <= year_max)]
    # Create date column
    population["date"] = pd.to_datetime(population["year"].astype("string") + "-07-01")
    # Keep relevant columns
    population = population.loc[:, ["date", "country", "population"]]
    # Add missing dates
    population = fill_date_gaps(population)
    # Linearly interpolate NaNs
    population = geo.interpolate_table(population, "country", "date")
    return cast(Table, population)


def add_population_daily(tb: Table, ds_population: Dataset, missing_countries: Optional[Set] = None) -> Table:
    """Add `population` column to table.

    Adds population value on a daily basis (extrapolated from yearly data).
    """
    countries_start = set(tb["country"].unique())
    tb_pop = make_table_population_daily(
        ds_population=ds_population, year_min=tb["date"].dt.year.min() - 1, year_max=tb["date"].dt.year.max() + 1
    )
    tb = tb.merge(tb_pop, on=["country", "date"])
    countries_end = set(tb["country"].unique())

    # Check countries that went missing
    if missing_countries is not None:
        countries_missing = countries_start - countries_end
        assert (
            countries_missing == missing_countries
        ), f"Missing countries don't match the expected! {countries_missing}"

    return tb


def make_monotonic(tb: Table, max_removed_rows=10) -> Table:
    n_rows_before = len(tb)
    dates_before = set(tb["date"])
    tb_before = tb.copy()

    tb = tb.sort_values("date")
    metrics = ("total_vaccinations", "people_vaccinated", "people_fully_vaccinated")
    tb[list(metrics)] = tb[list(metrics)].astype(float)
    for metric in metrics:
        while not tb[metric].ffill().fillna(0).is_monotonic_increasing:
            diff = tb[metric].ffill().shift(-1) - tb[metric].ffill()
            tb = tb.loc[(diff >= 0) | (diff.isna())]
    dates_now = set(tb.date)

    tb[list(metrics)] = tb[list(metrics)].astype("Int64")

    if max_removed_rows is not None:
        num_removed_rows = n_rows_before - len(tb)
        if num_removed_rows > max_removed_rows:
            dates_wrong = dates_before.difference(dates_now)
            tb_wrong = tb_before[tb_before.date.isin(dates_wrong)]
            raise Exception(
                f"{num_removed_rows} rows have been removed. That is more than maximum allowed ({max_removed_rows}) by"
                f" make_monotonic() - check the data. Check \n{tb_wrong}"  # {', '.join(sorted(dates_wrong))}"
            )

    return tb

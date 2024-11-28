import datetime as dt
from typing import Any, Dict, Hashable, Optional, Set, cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.processing import concat

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# NOTE: date format changed on 2024-09-01
# NOTE: date format changed back to %Y-%m-%d on 2024-09-05
DATE_FORMAT = "%Y-%m-%d"
# DATE_FORMAT = "%d/%m/%y"


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
    tb["date"] = pd.to_datetime(tb["date"], format=DATE_FORMAT).astype("datetime64[ns]")

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
    population = ds_population.read("population")
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


def add_population_2022(tb: Table, ds_population: Dataset, missing_countries: Optional[Set] = None) -> Table:
    """Add `population` column to table.

    Adds population value on a daily basis (extrapolated from yearly data).
    """
    year = 2022

    # save initial countries
    countries_start = set(tb["country"].unique())

    # load popultion from catalog
    tb_pop = ds_population["population"].reset_index()
    tb_pop = tb_pop.loc[tb_pop["year"] == year, ["country", "population"]].rename(
        columns={"population": "population_2022"}
    )

    # Define hardcoded population
    country_population = {
        "England": 57_112_500,  # https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/timeseries/enpop/pop
        "Wales": 3_132_700,  # https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/timeseries/wapop/pop
        "Northern Cyprus": 391_000,  # https://www.state.gov/reports/2022-report-on-international-religious-freedom/cyprus/area-administered-by-turkish-cypriots/#:~:text=According%20to%20a%20statement%20from,no%20data%20on%20religious%20affiliation.
        "Northern Ireland": 1_910_500,  # https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/timeseries/nipop/pop
        "Scotland": 5_447_700,  # https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/timeseries/scpop/pop
        "Pitcairn": 45,  # https://www.bbc.com/news/uk-56923016
    }
    tb_hc = Table.from_records([{"country": c, "population_2022": p} for c, p in country_population.items()])

    # Fix some values
    mask_cyprus = tb_pop["country"] == "Cyprus"
    tb_pop.loc[mask_cyprus, "population_2022"] = (
        tb_pop.loc[mask_cyprus, "population_2022"] - country_population["Northern Cyprus"]
    )

    # Combine both sources
    tb_pop = concat([tb_pop, tb_hc], ignore_index=True)

    # merge
    tb = tb.merge(tb_pop[["country", "population_2022"]], on=["country"])

    # save final countries (to compare w initial)
    countries_end = set(tb["country"].unique())

    # Check countries that went missing
    if missing_countries is not None:
        countries_missing = countries_start - countries_end
        assert (
            countries_missing == missing_countries
        ), f"Missing countries don't match the expected! {countries_missing}; expected {missing_countries}"

    return tb


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
    tb[list(metrics)] = tb[list(metrics)].astype("float64[pyarrow]")
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


def add_regions(
    tb: Table,
    ds_regions: Dataset,
    ds_income: Optional[Dataset] = None,
    keep_only_regions: bool = False,
    regions: Optional[Dict[Hashable, Any]] = None,
    **kwargs,
) -> Table:
    if regions is None:
        regions = {
            # Standard continents
            "Africa": {},
            "Asia": {},
            "Europe": {},
            "North America": {},
            "Oceania": {},
            "South America": {},
            # Income groups
            "Low-income countries": {},
            "Lower-middle-income countries": {},
            "Upper-middle-income countries": {},
            "High-income countries": {},
            # Special regions
            "European Union (27)": {},
            "World excl. China": {
                "additional_regions": ["Asia", "Africa", "Europe", "North America", "Oceania", "South America"],
                "excluded_members": ["China"],
            },
            "World excl. China and South Korea": {
                "additional_regions": ["Asia", "Africa", "Europe", "North America", "Oceania", "South America"],
                "excluded_members": ["China", "South Korea"],
            },
            "World excl. China, South Korea, Japan and Singapore": {
                "additional_regions": ["Asia", "Africa", "Europe", "North America", "Oceania", "South America"],
                "excluded_members": ["China", "South Korea", "Japan", "Singapore"],
            },
            "Asia excl. China": {
                "additional_regions": ["Asia"],
                "excluded_members": ["China"],
            },
        }
    # Regions
    tb = geo.add_regions_to_table(
        tb,
        ds_regions,
        ds_income,
        year_col="date",
        regions=regions,
        **kwargs,
    )
    # World
    tb = geo.add_regions_to_table(
        tb,
        ds_regions,
        ds_income,
        year_col="date",
        regions={"World": {}},
        **kwargs,
    )
    # Filter only regions if specified
    if keep_only_regions:
        tb = tb.loc[tb["country"].isin(set(regions) | {"World"})]
    return tb


def add_last12m_to_metric(tb: Table, column_metric: str, column_country: str = "country") -> Table:
    """Add last 12 month data for an indicator."""
    column_metric_12m = f"{column_metric}_last12m"

    # Get only last 12 month of data
    date_cutoff = dt.datetime.now() - dt.timedelta(days=365.2425)

    # Get metric value 12 months ago
    tb_tmp = (
        tb.loc[tb["date"] > date_cutoff]
        .dropna(subset=[column_metric])
        .sort_values([column_country, "date"])
        .drop_duplicates(column_country)[[column_country, column_metric]]
        .rename(columns={column_metric: column_metric_12m})
    )

    # Compute the difference, obtain last12m metric
    tb = tb.merge(tb_tmp, on=[column_country], how="left")
    tb[column_metric_12m] = tb[column_metric] - tb[column_metric_12m]

    # Assign NaN to >1 year old data
    tb[column_metric_12m].loc[tb["date"] < date_cutoff] = np.nan

    return tb

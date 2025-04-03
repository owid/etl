"""Load a meadow dataset and create a garden dataset."""

import datetime

import owid.catalog.processing as pr
import pandas as pd
import structlog
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = structlog.get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS_TO_ADD = [
    "North America",
    "South America",
    "Europe",
    "Asia",
    "Africa",
    "Oceania",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("monkeypox")
    ds_suspected = paths.load_dataset("global_health_mpox")
    # Read table from meadow dataset.
    tb = ds_meadow["monkeypox"].reset_index().astype({"country": str})
    tb_suspected = ds_suspected["global_health_mpox"].reset_index()
    cols = ["country", "date", "suspected_cases_cumulative"]
    tb_suspected = tb_suspected[cols]
    assert tb_suspected.shape[1] == len(cols)
    origins = tb["total_conf_cases"].metadata.origins
    #
    # Process data.
    #
    tb_orig = tb.copy()
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        make_missing_countries_nan=True,
    )

    ix = tb.country.isnull()
    missing_countries = set(tb_orig[ix].country)
    if missing_countries:
        log.warning(f"Missing countries in monkeypox.countries.json: {missing_countries}")
        tb.country = tb.country.fillna(tb_orig.country)

    tb = (
        tb.pipe(clean_columns)
        .pipe(clean_date)
        .pipe(clean_values)
        .pipe(explode_dates)
        .pipe(add_world)
        .pipe(add_regions)
        .pipe(add_population_and_countries)
        .pipe(derive_metrics)
        .pipe(filter_dates)
    )

    tb_both = pr.merge(tb, tb_suspected, on=["country", "date"], how="outer")

    # For variables on deaths we should show that data reported by the WHO show _only_ confirmed cases, in an annotation
    country_mask = tb_both["country"] == "Democratic Republic of Congo"
    tb_both["annotation"] = ""
    tb_both.loc[country_mask, "annotation"] = (
        tb_both.loc[country_mask, "annotation"] + "Includes only confirmed deaths as reported by WHO"
    )
    tb_both["annotation"].metadata.origins = origins
    tb_both = tb_both.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_both],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
        formats=["csv"],
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def clean_columns(tb: Table) -> Table:
    return tb.loc[:, ["country", "iso3", "date", "total_conf_cases", "total_conf_deaths"]].rename(
        columns={
            "date": "date",
            "total_conf_cases": "total_cases",
            "total_conf_deaths": "total_deaths",
            "iso3": "iso_code",
        }
    )


def clean_date(tb: Table) -> Table:
    tb["date"] = pd.to_datetime(tb.date).dt.date.astype(str)
    return tb


def clean_values(tb: Table) -> Table:
    tb = tb.sort_values("date", ascending=False)
    tb["total_cases"] = tb[["country", "total_cases"]].groupby("country", observed=True).cummin()
    tb["total_deaths"] = tb[["country", "total_deaths"]].groupby("country", observed=True).cummin()
    return tb.sort_values(["country", "date"])


def explode_dates(tb: Table) -> Table:
    tb_range = pd.concat(
        [
            pd.DataFrame(
                {
                    "country": country,
                    "date": pd.date_range(start=tb.date.min(), end=tb.date.max(), freq="D").astype(str),
                }
            )
            for country in tb.country.unique()
        ]
    )
    country_to_iso = tb[["country", "iso_code"]].drop_duplicates().set_index("country").iso_code
    tb = pr.merge(tb, Table(tb_range), on=["country", "date"], validate="one_to_one", how="right")
    tb["iso_code"] = tb.country.map(country_to_iso)
    tb["report"] = tb.total_cases.notnull() | tb.total_deaths.notnull()
    return tb


def get_last_date_with_more_than_ten_countries_reporting(tb: Table) -> str:
    # Experiment to _not_ add world data for recent dates when there is a lag in reporting
    tb["date"] = pd.to_datetime(tb.date).astype(str)
    tb_reporting = tb[tb["report"]]
    # how many countries reporting each day
    num_reporting = tb_reporting.groupby("date").country.nunique().reset_index(name="countries_reporting")
    last_date_with_more_than_ten_countries_reporting = num_reporting[num_reporting["countries_reporting"] > 10][
        "date"
    ].max()

    return last_date_with_more_than_ten_countries_reporting


def add_world(tb: Table) -> Table:
    last_date = get_last_date_with_more_than_ten_countries_reporting(tb)

    tb[["total_cases", "total_deaths"]] = (
        tb[["country", "total_cases", "total_deaths"]].groupby("country", observed=True).ffill().fillna(0)
    )

    world = (
        tb[["date", "total_cases", "total_deaths"]][tb["date"] >= last_date]
        .groupby("date", as_index=False, observed=True)
        .sum()
        .assign(country="World", report=True)
    )
    world = world[world.date < str(datetime.date.today())]
    return pr.concat([tb, world])


def add_regions(tb: Table) -> Table:
    ds_regions = paths.load_dataset("regions")
    last_date = get_last_date_with_more_than_ten_countries_reporting(tb)
    # Add region for each country
    for region in REGIONS_TO_ADD:
        countries_in_region = geo.list_members_of_region(
            region=region,
            ds_regions=ds_regions,
        )
        tb.loc[tb.country.isin(countries_in_region), "region"] = region

    # Calculate regional aggregates
    regions = (
        tb[(tb.region.notnull()) & (tb["date"] >= last_date)][
            ["region", "date", "total_cases", "total_deaths", "report"]
        ]
        .groupby(["region", "date"], as_index=False, observed=True)
        .agg({"total_cases": "sum", "total_deaths": "sum", "report": "max"})
        .rename(columns={"region": "country"})
    )
    regions = regions[regions.date < str(datetime.date.today())]
    tb = tb.drop(columns="region")

    # Add iso codes to regions
    region_to_iso = ds_regions["regions"].reset_index().set_index("name").code
    regions["iso_code"] = regions.country.map(region_to_iso)

    # Concatenate with tb
    return pr.concat([tb, regions])


def add_population_and_countries(tb: Table) -> Table:
    ds_population = paths.load_dataset("population")
    tb = geo.add_population_daily(tb, ds_population)
    tb.date = tb.date.dt.date.astype(str)
    return tb


def derive_metrics(tb: Table) -> Table:
    def derive_country_metrics(tb: Table) -> Table:
        # Add daily values
        tb["new_cases"] = tb.total_cases.diff()
        tb["new_deaths"] = tb.total_deaths.diff()

        # Add 7-day averages
        tb["new_cases_smoothed"] = tb.new_cases.rolling(window=7, min_periods=7, center=False).mean().round(2)
        tb["new_deaths_smoothed"] = tb.new_deaths.rolling(window=7, min_periods=7, center=False).mean().round(2)

        # Add per-capita metrics
        tb = tb.assign(
            new_cases_per_million=round(tb.new_cases * 1000000 / tb.population, 3),
            total_cases_per_million=round(tb.total_cases * 1000000 / tb.population, 3),
            new_cases_smoothed_per_million=round(tb.new_cases_smoothed * 1000000 / tb.population, 3),
            new_deaths_per_million=round(tb.new_deaths * 1000000 / tb.population, 5),
            total_deaths_per_million=round(tb.total_deaths * 1000000 / tb.population, 5),
            new_deaths_smoothed_per_million=round(tb.new_deaths_smoothed * 1000000 / tb.population, 5),
        ).drop(columns="population")

        min_reporting_date = tb[tb.report].date.min()
        max_reporting_date = tb[tb.report].date.max()
        tb = tb[(tb.date >= min_reporting_date) & (tb.date <= max_reporting_date)].drop(columns="report")

        return tb

    return tb.groupby("country", observed=True, group_keys=False).apply(derive_country_metrics)


def filter_dates(tb: Table) -> Table:
    return tb[tb.date >= "2022-05-01"]

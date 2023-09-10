import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Base URL for Economist excess mortality data.
BASE_URL = (
    "https://raw.githubusercontent.com/TheEconomist/covid-19-the-economist-global-excess-deaths-model/main/output-data"
)


def run(dest_dir: str) -> None:
    #
    # Load data from Github.
    #
    country_all = _load_country_data()
    world_all = _load_world_data()
    df = _combine_data(country_all, world_all)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb = tb.set_index(["country", "date"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = create_dataset(dest_dir, tables=[tb])

    # Save changes in the new garden dataset.
    ds_garden.save()


def _load_country_data():
    # load in the data
    country_wk = pd.read_csv(
        f"{BASE_URL}/export_country.csv",
        usecols=[
            "iso3c",
            "date",
            "estimated_daily_excess_deaths",
            "estimated_daily_excess_deaths_ci_95_top",
            "estimated_daily_excess_deaths_ci_95_bot",
        ],  # type: ignore
    )
    country_wk_100k = pd.read_csv(
        f"{BASE_URL}/export_country_per_100k.csv",
        usecols=[
            "iso3c",
            "date",
            "estimated_daily_excess_deaths_per_100k",
            "estimated_daily_excess_deaths_ci_95_top_per_100k",
            "estimated_daily_excess_deaths_ci_95_bot_per_100k",
        ],  # type: ignore
    )

    country_cum = pd.read_csv(
        f"{BASE_URL}/export_country_cumulative.csv",
        usecols=[
            "iso3c",
            "date",
            "cumulative_estimated_daily_excess_deaths",
            "cumulative_estimated_daily_excess_deaths_ci_95_top",
            "cumulative_estimated_daily_excess_deaths_ci_95_bot",
        ],  # type: ignore
    )
    country_cum_100k = pd.read_csv(
        f"{BASE_URL}/export_country_per_100k_cumulative.csv",
        usecols=[
            "iso3c",
            "date",
            "cumulative_estimated_daily_excess_deaths_per_100k",
            "cumulative_estimated_daily_excess_deaths_ci_95_top_per_100k",
            "cumulative_estimated_daily_excess_deaths_ci_95_bot_per_100k",
        ],  # type: ignore
    )

    # this file tells whether reported data is available
    reported = pd.read_csv(f"{BASE_URL}/output-for-interactive/by_location_full_data.csv")
    report_select = reported[reported["type"] == "daily_excess_deaths"][["iso3c", "date", "known_excess_deaths"]]

    # get rid of NA rows
    report_select = report_select[report_select["iso3c"].notna()]

    # merge all the data
    country_wk_merge = pd.merge(country_wk, country_wk_100k, on=["iso3c", "date"], how="outer")
    country_cum_merge = pd.merge(country_cum, country_cum_100k, on=["iso3c", "date"], how="outer")
    country_almost = pd.merge(country_cum_merge, country_wk_merge, on=["iso3c", "date"], how="outer")
    country_all = pd.merge(country_almost, report_select, on=["iso3c", "date"], how="outer")

    # the most recent date can have null known_excess_deaths, fill them with False
    country_all["known_excess_deaths"] = country_all["known_excess_deaths"].fillna(False)

    return country_all


def _load_world_data():
    world_wk = pd.read_csv(
        f"{BASE_URL}/export_world.csv",
        usecols=[
            "world",
            "date",
            "estimated_daily_excess_deaths",
            "estimated_daily_excess_deaths_ci_95_top",
            "estimated_daily_excess_deaths_ci_95_bot",
        ],  # type: ignore
    )
    world_wk_100k = pd.read_csv(
        f"{BASE_URL}/export_world_per_100k.csv",
        usecols=[
            "world",
            "date",
            "estimated_daily_excess_deaths_per_100k",
            "estimated_daily_excess_deaths_ci_95_top_per_100k",
            "estimated_daily_excess_deaths_ci_95_bot_per_100k",
        ],  # type: ignore
    )

    world_cum = pd.read_csv(
        f"{BASE_URL}/export_world_cumulative.csv",
        usecols=[
            "world",
            "date",
            "cumulative_estimated_daily_excess_deaths",
            "cumulative_estimated_daily_excess_deaths_ci_95_top",
            "cumulative_estimated_daily_excess_deaths_ci_95_bot",
        ],  # type: ignore
    )
    world_cum_100k = pd.read_csv(
        f"{BASE_URL}/export_world_per_100k_cumulative.csv",
        usecols=[
            "world",
            "date",
            "cumulative_estimated_daily_excess_deaths_per_100k",
            "cumulative_estimated_daily_excess_deaths_ci_95_top_per_100k",
            "cumulative_estimated_daily_excess_deaths_ci_95_bot_per_100k",
        ],  # type: ignore
    )

    world_wk_merge = pd.merge(world_wk, world_wk_100k, on=["world", "date"], how="outer")
    world_cum_merge = pd.merge(world_cum, world_cum_100k, on=["world", "date"], how="outer")

    world_all = pd.merge(world_cum_merge, world_wk_merge, on=["world", "date"], how="outer")

    return world_all


def _combine_data(country_all: pd.DataFrame, world_all: pd.DataFrame) -> pd.DataFrame:
    # rename columns and bind rows into one
    country_all.rename(columns={"iso3c": "country"}, inplace=True)
    world_all.rename(columns={"world": "country"}, inplace=True)

    combined_data = pd.concat([country_all, world_all], ignore_index=True)

    # these should be replaced with FALSE values
    combined_data["known_excess_deaths"].fillna(False, inplace=True)

    # if reported data exists (TRUE), collapse uncertainty interval to central estimate
    # if reported data does not exist (FALSE), keep uncertainty interval as is
    known_deaths = combined_data["known_excess_deaths"]
    combined_data.loc[known_deaths, "estimated_daily_excess_deaths_ci_95_top"] = combined_data.loc[
        known_deaths, "estimated_daily_excess_deaths"
    ]
    combined_data.loc[known_deaths, "estimated_daily_excess_deaths_ci_95_bot"] = combined_data.loc[
        known_deaths, "estimated_daily_excess_deaths"
    ]
    combined_data.loc[known_deaths, "estimated_daily_excess_deaths_ci_95_top_per_100k"] = combined_data.loc[
        known_deaths, "estimated_daily_excess_deaths_per_100k"
    ]
    combined_data.loc[known_deaths, "estimated_daily_excess_deaths_ci_95_bot_per_100k"] = combined_data.loc[
        known_deaths, "estimated_daily_excess_deaths_per_100k"
    ]

    return combined_data

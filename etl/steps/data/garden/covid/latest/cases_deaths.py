"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from shared import add_population_daily, fill_date_gaps

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
    ds_regions = paths.load_dataset("regions")
    ds_income = paths.load_dataset("income_groups")
    ds_population = paths.load_dataset("population")

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
    # Fill NaNs:
    # - Filling in NaNs with zeroes, for daily indicators.
    # - Filling in NaNs with the last non-NaN value, for cumulative indicators (forward filling).
    tb[["new_cases", "new_deaths"]] = tb[["new_cases", "new_deaths"]].fillna(0)
    tb[["total_cases", "total_deaths"]] = tb.groupby("country")[["total_cases", "total_deaths"]].ffill()  # type: ignore

    # Main processing
    tb["date"] = pd.to_datetime(tb["date"], format="%Y-%m-%d")

    # Remaining processing
    ## Drop rows
    tb = discard_rows(tb)
    ## Add population
    tb = add_population_daily(
        tb,
        ds_population,
        missing_countries={
            "International",
            "Pitcairn",
        },
    )
    ## Add regions
    tb = add_regions(tb, ds_regions, ds_income)
    ## Add period-aggregtes, doublig days
    tb = add_period_aggregates(tb, "weekly", 7)
    tb = add_period_aggregates(tb, "biweekly", 14)
    # tb = add_doubling_days(tb)
    ## Per-capita indicators
    tb = add_per_capita(tb)
    ## Add rolling averages
    tb = add_rolling_avg(tb)
    ## Add CFR
    tb = add_cfr(tb)
    ## 'Days since' indicators
    tb = add_days_since(tb)
    ## Add Exemplars indicators
    tb = add_exemplars(tb)
    ## Drop population
    tb = tb.drop(columns=["population"])
    ## Dtypes
    tb = set_dtypes(tb)

    # Format
    tb = tb.format(["country", "date"])

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


def discard_rows(tb: Table):
    """Discard outliers."""
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

    # Sort (legacy)
    tb = tb.sort_values(["country", "date"])

    return tb


def add_regions(tb: Table, ds_regions: Dataset, ds_income: Dataset) -> Table:
    tb = geo.add_regions_to_table(
        tb,
        ds_regions,
        ds_income,
        year_col="date",
        regions={
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
        },
    )
    tb = geo.add_regions_to_table(
        tb,
        ds_regions,
        ds_income,
        year_col="date",
        regions={"World": {}},
    )
    return tb


def add_period_aggregates(tb: Table, prefix: str, periods: int):
    # Period-aggregate cases and deaths
    cases_colname = f"{prefix}_cases"
    deaths_colname = f"{prefix}_deaths"
    tb[[cases_colname, deaths_colname]] = (
        tb[["country", "new_cases", "new_deaths"]]
        .groupby("country")[["new_cases", "new_deaths"]]
        .rolling(window=periods, min_periods=periods - 1, center=False)
        .sum()
        .reset_index(level=0, drop=True)
    )

    # Period-growth of cases and deaths
    cases_growth_colname = f"{prefix}_pct_growth_cases"
    deaths_growth_colname = f"{prefix}_pct_growth_deaths"
    tb[[cases_growth_colname, deaths_growth_colname]] = (
        tb[["country", cases_colname, deaths_colname]]
        .groupby("country")[[cases_colname, deaths_colname]]
        .pct_change(periods=periods, fill_method=None)
        .round(3)
        .replace([np.inf, -np.inf], pd.NA)
        * 100
    )

    # Set NaNs where the original data was NaN
    tb.loc[tb["new_cases"].isnull(), cases_colname] = np.nan
    tb.loc[tb["new_deaths"].isnull(), deaths_colname] = np.nan

    return tb


def add_doubling_days(tb: Table) -> Table:
    paths.log.info("Adding doubling days…")

    DOUBLING_DAYS_SPEC = {
        "doubling_days_total_cases_3_day_period": {
            "value_col": "total_cases",
            "periods": 3,
        },
        "doubling_days_total_cases_7_day_period": {
            "value_col": "total_cases",
            "periods": 7,
        },
        "doubling_days_total_deaths_3_day_period": {
            "value_col": "total_deaths",
            "periods": 3,
        },
        "doubling_days_total_deaths_7_day_period": {
            "value_col": "total_deaths",
            "periods": 7,
        },
    }

    for col, spec in DOUBLING_DAYS_SPEC.items():
        value_col = spec["value_col"]
        periods = spec["periods"]
        tb.loc[tb[value_col] == 0, value_col] = np.nan
        tb[col] = (
            tb.groupby("country", as_index=False)[value_col]
            .pct_change(periods=periods, fill_method=None)
            .map(lambda pct: pct_change_to_doubling_days(pct, periods))
        )
    return tb


def pct_change_to_doubling_days(pct_change, periods):
    if pd.notnull(pct_change) and pct_change != 0:
        doubling_days = periods * np.log(2) / np.log(1 + pct_change)
        return np.round(doubling_days, decimals=2)
    return pd.NA


def add_per_capita(tb: Table) -> Table:
    """Add per-million-capita indicators."""
    paths.log.info("Add per-capita indicators…")
    # Fix population value for France (Should not include overseas territories for the WHO)
    tb.loc[tb["country"] == "France", "population"] -= 2e6
    # Estimate per-million-capita indicator variants
    indicators = [
        "new_cases",
        "new_deaths",
        "total_cases",
        "total_deaths",
        "weekly_cases",
        "weekly_deaths",
        "biweekly_cases",
        "biweekly_deaths",
    ]
    for indicator in indicators:
        tb[f"{indicator}_per_million"] = tb[indicator] / (tb["population"] / 1_000_000)
    return tb


def add_rolling_avg(tb: Table) -> Table:
    """Add rolling average for new cases and deaths (per-capita too)."""
    paths.log.info("Adding rolling averages…")
    indicators = [
        "new_cases",
        "new_deaths",
        "new_cases_per_million",
        "new_deaths_per_million",
    ]
    tb = tb.copy().sort_values(by="date")

    for indicator in indicators:
        col = f"{indicator}_7_day_avg_right"
        tb[col] = tb[indicator].astype("float")
        tb[col] = (
            tb.groupby("country")[col]
            .rolling(
                window=7,
                min_periods=6,
                center=False,
            )
            .mean()
            .reset_index(level=0, drop=True)
        )

    for indicator in indicators:
        col = f"{indicator}_7_day_avg_right"
        tb[col] = tb[col].copy_metadata(tb[indicator])

    return tb


def add_cfr(tb: Table) -> Table:
    """Add CFR."""
    paths.log.info("Adding case-fatality-rate indicators...")

    def _apply_row_cfr_100(row):
        if pd.notnull(row["total_cases"]) and row["total_cases"] >= 100:
            return row["cfr"]
        return pd.NA

    tb["cfr"] = 100 * tb["total_deaths"] / tb["total_cases"]
    tb["cfr_100_cases"] = tb.apply(_apply_row_cfr_100, axis=1)
    tb["cfr_100_cases"] = tb["cfr_100_cases"].copy_metadata(tb["cfr"])

    # Replace inf
    tb["cfr"] = tb["cfr"].replace([np.inf, -np.inf], pd.NA)
    tb["cfr_100_cases"] = tb["cfr_100_cases"].replace([np.inf, -np.inf], pd.NA)
    return tb


def add_days_since(tb: Table) -> Table:
    """Add 'days since'-type indicators."""
    paths.log.info("Adding days-since indicators...")
    indicators = [
        ("total_cases", 100),
        ("total_deaths", 5),
        ("total_cases_per_million", 1),
        ("total_deaths_per_million", 0.1),
    ]

    def _days_since(tb, indicator_name, threshold):
        def _get_date_of_threshold(tb, col, threshold):
            try:
                return tb["date"][tb[col] >= threshold].iloc[0]
            except Exception:
                return None

        def _date_diff(a, b):
            if pd.isnull(a) or pd.isnull(b):
                return None
            diff = (a - b).days
            return diff

        ref_date = pd.to_datetime(_get_date_of_threshold(tb, indicator_name, threshold))
        return pd.to_datetime(tb["date"]).map(lambda date: _date_diff(date, ref_date)).astype("Int64")

    tb = tb.copy()
    for indicator in indicators:
        col = f"days_since_{indicator[1]}_{indicator[0]}".replace(".", "_")
        tb[col] = (
            tb[["date", "country", indicator[0]]]
            .groupby("country")
            .apply(lambda df_group: _days_since(df_group, indicator[0], indicator[1]))
            .reset_index(level=0, drop=True)
        )
        tb[col] = tb[col].copy_metadata(tb[indicator[0]])

    return tb


def add_exemplars(tb: Table):
    paths.log.info("Adding exemplars metrics…")

    # Inject days since 100th case IF population ≥ 5M
    def mapper_days_since(row):
        if pd.notnull(row["population"]) and row["population"] >= 5e6:
            return row["days_since_100_total_cases"]
        return pd.NA

    tb["days_since_100_total_cases_and_5m_pop"] = tb.apply(func=mapper_days_since, axis=1)
    tb["days_since_100_total_cases_and_5m_pop"] = tb["days_since_100_total_cases_and_5m_pop"].copy_metadata(
        tb["days_since_100_total_cases"]
    )

    return tb


def set_dtypes(tb: Table) -> Table:
    """Set Dtypes for the table."""
    dtypes = {
        "country": "string",
        "date": "datetime64[ns]",
        **{
            col: "Int64"
            for col in {
                "new_cases",
                "total_cases",
                "new_deaths",
                "total_deaths",
                "weekly_cases",
                "weekly_deaths",
                "biweekly_cases",
                "biweekly_deaths",
                "days_since_100_total_cases_and_5m_pop",
            }
        },
        **{
            col: "Float64"
            for col in {
                "cfr",
                "cfr_100_cases",
                # "doubling_days_total_deaths_7_day_period",
                # "doubling_days_total_deaths_3_day_period",
                # "doubling_days_total_cases_7_day_period",
                # "doubling_days_total_cases_3_day_period",
                "weekly_pct_growth_cases",
                "weekly_pct_growth_deaths",
                "biweekly_pct_growth_deaths",
                "biweekly_pct_growth_cases",
            }
        },
    }
    return tb.astype(dtypes)

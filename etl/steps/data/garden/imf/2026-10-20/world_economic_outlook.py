"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

RELEASE_YEAR = int(paths.version.split("-")[0])

# Indicators, each split into observations and forecasts.
INDICATORS = ["gross_domestic_product__gdp__constant_prices__percent_change", "unemployment_rate"]

# Fiscal years the IMF reports, and whether each one is shown in the year it starts (True) or ends (False).
FISCAL_YEAR_STARTS_IN_FIRST_YEAR = {
    "April/March": True,
    "July/June": False,
    "August/July": False,
    "October/September": False,
}

# Minimum number of countries and country groups expected (207 in the April and October 2026 releases).
MIN_ENTITIES = 200


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("world_economic_outlook")

    # Read table from meadow dataset.
    tb = ds_meadow.read("world_economic_outlook")

    #
    # Process data.
    #
    # Step 1: Convert the IMF's latest year of actual data to a calendar year.
    tb = add_latest_actual_year(tb)

    # Step 2: Split each indicator into observations and forecasts, and reshape to wide format.
    tb = split_observations_and_forecasts(tb)

    # Step 3: Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Step 4: Apply (or log) known source errors.
    tb = paths.apply_corrections(tb)

    # Check the output is plausible.
    sanity_check_outputs(tb)

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_latest_actual_year(tb: Table) -> Table:
    """
    Converts the IMF's latest year of actual data (a year, or a fiscal-year label like FY2025/26) to an integer year.
    """
    tb["country"] = tb["country"].astype(str)

    # The fiscal-year months are only reported on the GDP series, so take them per country.
    fiscal_year_months = (
        tb.dropna(subset=["start_end_months_of_reporting_year"])
        .drop_duplicates(subset=["country"])
        .set_index("country")["start_end_months_of_reporting_year"]
        .to_dict()
    )

    tb["latest_actual_annual_data"] = [
        convert_to_year(label, fiscal_year_months.get(country))
        for label, country in zip(tb["latest_actual_annual_data"], tb["country"])
    ]

    # The IMF reports no latest year of actual data for most of its country groups. In its own tables, the current
    # year and later years are projections, so the previous year is the last one of actual data.
    tb["latest_actual_annual_data"] = tb["latest_actual_annual_data"].fillna(RELEASE_YEAR - 1).astype("Int64")

    # Data for the release year itself is never complete, so it should be a projection (except for fiscal years that
    # ended earlier in the release year, like Bangladesh's FY2025/26 from July to June, shown as 2026).
    calendar_year = tb["country"].map(fiscal_year_months).fillna("January/December") == "January/December"
    assert (tb.loc[calendar_year, "latest_actual_annual_data"] < RELEASE_YEAR).all(), (
        "Some calendar-year series have actual data for the release year."
    )
    assert (tb["latest_actual_annual_data"] <= RELEASE_YEAR).all(), (
        "Some series have actual data after the release year."
    )

    return tb.drop(columns="start_end_months_of_reporting_year")


def convert_to_year(label, fiscal_year_months):
    """
    Converts the IMF's latest year of actual data to the year column the IMF shows that data in.

    A fiscal year goes in the calendar year that holds most of its months, and in the year it ends when the split is
    even: FY2025/26 from April to March is shown as 2025 (as the IMF does for India), and FY2024/25 from July to June
    as 2025 (as for Egypt).
    """
    if pd.isna(label):
        return pd.NA
    if isinstance(label, str) and label.startswith("FY"):
        start_year = int(label[2:6])
        assert fiscal_year_months in FISCAL_YEAR_STARTS_IN_FIRST_YEAR, (
            f"Unexpected fiscal year months for {label}: {fiscal_year_months}."
        )
        if FISCAL_YEAR_STARTS_IN_FIRST_YEAR[fiscal_year_months]:
            return start_year
        return start_year + 1
    return int(label)


def split_observations_and_forecasts(tb: Table) -> Table:
    """
    Splits each indicator into observations (up to the latest year of actual data) and forecasts, in wide format.
    """
    tb["indicator"] = tb["indicator"].astype(str)
    tb.loc[tb["year"] > tb["latest_actual_annual_data"], "indicator"] += "_forecast"
    tb.loc[tb["year"] <= tb["latest_actual_annual_data"], "indicator"] += "_observation"

    tb = tb.dropna(subset=["value"]).drop(columns="latest_actual_annual_data")
    tb = tb.pivot(index=["country", "year"], columns="indicator", values="value", join_column_levels_with="_")

    return tb.underscore()


def sanity_check_outputs(tb: Table) -> None:
    expected_columns = {f"{indicator}_{kind}" for indicator in INDICATORS for kind in ["observation", "forecast"]}
    assert set(tb.columns) - {"country", "year"} == expected_columns, f"Unexpected columns: {set(tb.columns)}."
    assert tb["country"].nunique() >= MIN_ENTITIES, f"Only {tb['country'].nunique()} countries and country groups."
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."

    for indicator in INDICATORS:
        observation, forecast = tb[f"{indicator}_observation"], tb[f"{indicator}_forecast"]
        # A year is either an observation or a forecast, never both.
        assert not (observation.notna() & forecast.notna()).any(), f"{indicator}: years that are both kinds."
        # Observations always come before forecasts.
        last_observation = tb[observation.notna()].groupby("country")["year"].max()
        first_forecast = tb[forecast.notna()].groupby("country")["year"].min()
        both = last_observation.index.intersection(first_forecast.index)
        assert (last_observation[both] < first_forecast[both]).all(), f"{indicator}: forecasts before observations."

    # Projections start in the release year for the world.
    world = tb[tb["country"] == "World"]
    gdp = INDICATORS[0]
    assert world.loc[world[f"{gdp}_observation"].notna(), "year"].max() == RELEASE_YEAR - 1, "World: wrong last year."
    assert world.loc[world[f"{gdp}_forecast"].notna(), "year"].min() == RELEASE_YEAR, "World: wrong first projection."

    # Value ranges (growth reached 148% in Equatorial Guinea in 1997, and -54% in Macao in 2020).
    for kind in ["observation", "forecast"]:
        growth = tb[f"{gdp}_{kind}"].dropna()
        assert growth.between(-70, 200).all(), f"GDP growth out of range: {growth.min()} to {growth.max()}."
        unemployment = tb[f"unemployment_rate_{kind}"].dropna()
        assert unemployment.between(0, 100).all(), f"Unemployment rate out of range: {unemployment.max()}."

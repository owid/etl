"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Indicator codes in the IMF API, and the series names the IMF gives them (which become indicator names).
INDICATOR_NAMES = {
    "NGDP_RPCH": "Gross domestic product (GDP), Constant prices, Percent change",
    "LUR": "Unemployment rate",
}

# Columns to keep from the SDMX-CSV file, and their new names.
COLUMNS = {
    "COUNTRY": "country",
    "INDICATOR": "indicator",
    "TIME_PERIOD": "year",
    "OBS_VALUE": "value",
    "LATEST_ACTUAL_ANNUAL_DATA": "latest_actual_annual_data",
}

RELEASE_YEAR = int(paths.version.split("-")[0])

# Fiscal years the IMF reports, and whether each one is shown in the year it starts (True) or ends (False).
FISCAL_YEAR_STARTS_IN_FIRST_YEAR = {
    "April/March": True,
    "July/June": False,
    "August/July": False,
    "October/September": False,
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("world_economic_outlook.csv")

    # Load data from snapshot.
    tb = snap.read_csv(
        usecols=list(COLUMNS) + ["FREQUENCY", "SERIES_NAME", "START_END_MONTHS_OF_REPORTING_YEAR"], low_memory=False
    )

    #
    # Process data.
    #
    # Step 1: Check the indicators and frequency are the expected ones.
    sanity_check_inputs(tb)

    # Step 2: Keep relevant columns, and use the IMF's series names for indicators.
    tb = prepare_data(tb)

    # Step 3: Split each indicator into observations and forecasts, and reshape to wide format.
    tb = reshape_and_clean(tb)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def sanity_check_inputs(tb: Table) -> None:
    assert set(tb["FREQUENCY"]) == {"A"}, f"Expected only annual data, got {set(tb['FREQUENCY'])}."
    assert set(tb["INDICATOR"]) == set(INDICATOR_NAMES), f"Unexpected indicator codes: {set(tb['INDICATOR'])}."
    series_names = tb[["INDICATOR", "SERIES_NAME"]].drop_duplicates().set_index("INDICATOR")["SERIES_NAME"].to_dict()
    assert series_names == INDICATOR_NAMES, f"The IMF changed its series names: {series_names}."
    assert not tb.duplicated(subset=["COUNTRY", "INDICATOR", "TIME_PERIOD"]).any(), "Duplicate rows in the snapshot."


def prepare_data(tb: Table) -> Table:
    """
    Keeps the relevant columns and converts the latest year of actual data to an integer year.
    """
    # The fiscal-year months are only reported on the GDP series, so take them per country.
    fiscal_year_months = (
        tb.dropna(subset=["START_END_MONTHS_OF_REPORTING_YEAR"])
        .drop_duplicates(subset=["COUNTRY"])
        .set_index("COUNTRY")["START_END_MONTHS_OF_REPORTING_YEAR"]
        .to_dict()
    )

    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")
    tb["indicator"] = tb["indicator"].map(INDICATOR_NAMES)

    # Format latest_actual_annual_data as integer.
    # Some countries report on a fiscal-year basis, with labels like FY2025/26.
    tb["latest_actual_annual_data"] = [
        convert_to_year(label, fiscal_year_months.get(country))
        for label, country in zip(tb["latest_actual_annual_data"], tb["country"])
    ]

    # The IMF reports no latest year of actual data for most of its country groups. In its own tables, the current
    # year and later years are projections, so the previous year is the last one of actual data.
    tb["latest_actual_annual_data"] = tb["latest_actual_annual_data"].fillna(RELEASE_YEAR - 1)

    # Data for the release year itself is never complete, so it should be a projection (except for fiscal years that
    # ended earlier in the release year, like Bangladesh's FY2025/26 from July to June, shown as 2026).
    calendar_year = tb["country"].map(fiscal_year_months).fillna("January/December") == "January/December"
    assert (tb.loc[calendar_year, "latest_actual_annual_data"] < RELEASE_YEAR).all(), (
        "Some calendar-year series have actual data for the release year."
    )
    assert (tb["latest_actual_annual_data"] <= RELEASE_YEAR).all(), (
        "Some series have actual data after the release year."
    )

    return tb


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


def reshape_and_clean(tb: Table) -> Table:
    """
    Splits each indicator into observations and forecasts, and reshapes the table to wide format.
    """
    # Coerce values to numeric.
    tb["year"] = tb["year"].astype("Int64")
    tb["latest_actual_annual_data"] = tb["latest_actual_annual_data"].astype("Int64")

    # Split between observations and forecasts
    tb.loc[tb.year > tb["latest_actual_annual_data"], "indicator"] += "_forecast"
    tb.loc[tb.year <= tb["latest_actual_annual_data"], "indicator"] += "_observation"

    # Drop rows with missing values.
    tb = tb.dropna(subset=["value"])

    # Drop latest_actual_annual_data
    tb = tb.drop(columns="latest_actual_annual_data")

    tb = tb.pivot(
        index=["country", "year"],
        columns="indicator",
        values="value",
        join_column_levels_with="_",
    )

    return tb

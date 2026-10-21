"""Load a snapshot and create a meadow dataset."""

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
    "START_END_MONTHS_OF_REPORTING_YEAR": "start_end_months_of_reporting_year",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("world_economic_outlook.csv")

    # Load data from snapshot.
    tb = snap.read_csv(usecols=list(COLUMNS) + ["FREQUENCY", "SERIES_NAME"], low_memory=False)

    #
    # Process data.
    #
    # Step 1: Check the indicators and frequency are the expected ones.
    sanity_check_inputs(tb)

    # Step 2: Keep relevant columns, and use the IMF's series names for indicators.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")
    tb["indicator"] = tb["indicator"].map(INDICATOR_NAMES)
    for column in ["country", "indicator"]:
        tb[column] = tb[column].astype("category")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "indicator"])

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

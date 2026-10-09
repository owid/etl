"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Indicator codes in the IMF API, and the series names the IMF gives them (which become indicator names in garden).
INDICATOR_NAMES = {
    "XG_FOB_USD": "Exports of goods, Free on board (FOB), US dollar",
    "MG_CIF_USD": "Imports of goods, Cost insurance freight (CIF), US dollar",
    "MG_FOB_USD": "Imports of goods, Free on board (FOB), US dollar",
    "TBG_USD": "Trade balance goods, US dollar",
}

# Columns to keep from the SDMX-CSV files, and their new names.
COLUMNS = {
    "COUNTRY": "country",
    "INDICATOR": "indicator",
    "COUNTERPART_COUNTRY": "counterpart_country",
    "TIME_PERIOD": "year",
    "OBS_VALUE": "value",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("trade.zip")

    # Load data from snapshot (one CSV file per indicator).
    with snap.extracted() as archive:
        tables = [
            archive.read(f"{indicator}.csv", usecols=list(COLUMNS) + ["FREQUENCY"], dtype={"OBS_VALUE": float})
            for indicator in INDICATOR_NAMES
        ]
    tb = pr.concat(tables, ignore_index=True)

    #
    # Process data.
    #
    # Sanity checks.
    assert set(tb["FREQUENCY"]) == {"A"}, "Expected only annual data."
    assert set(tb["INDICATOR"]) == set(INDICATOR_NAMES), "Unexpected indicator codes."

    # Keep relevant columns, and use the IMF's series names for indicators.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")
    tb["indicator"] = tb["indicator"].map(INDICATOR_NAMES)

    # Use categoricals to reduce memory and file size.
    for column in ["country", "indicator", "counterpart_country"]:
        tb[column] = tb[column].astype("category")

    tables = [tb.format(["country", "year", "indicator", "counterpart_country"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

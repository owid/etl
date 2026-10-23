"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# SDMX-CSV columns that carry no information once the file is loaded: the dataflow id, its last-update stamp and
# the frequency (always annual).
COLUMNS_TO_DROP = ["DATAFLOW", "LAST UPDATE", "freq"]
COLUMNS_TO_RENAME = {
    "geo": "country",
    "TIME_PERIOD": "year",
    "OBS_VALUE": "value",
    "OBS_FLAG": "flag",
    "CONF_STATUS": "confidentiality",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and read its gzipped SDMX-CSV file.
    snap = paths.load_snapshot("pension_entitlements.gz")
    tb = snap.read_csv(compression="gzip")

    #
    # Process data.
    #
    # Step 1: Drop the columns that carry no information.
    assert set(tb["freq"]) == {"A"}, f"Unexpected frequencies: {set(tb['freq'])}"
    # Eurostat withholds confidential values ("C"), so those rows must have no value.
    assert set(tb["CONF_STATUS"].dropna()) <= {"C"}, f"Unexpected statuses: {set(tb['CONF_STATUS'].dropna())}"
    assert tb.loc[tb["CONF_STATUS"].notna(), "OBS_VALUE"].isna().all(), "A confidential row carries a value."
    tb = tb.drop(columns=COLUMNS_TO_DROP, errors="raise").rename(columns=COLUMNS_TO_RENAME, errors="raise")

    # Step 2: Cast the low-cardinality columns to categoricals, and set the index.
    for column in ["country", "unit", "penscheme", "na_item", "flag", "confidentiality"]:
        tb[column] = tb[column].astype("category")
    tb = tb.format(["country", "year", "unit", "penscheme", "na_item"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from the data, and how to rename them.
COLUMNS = {
    "short_desc": "data_item",
    "year": "year",
    "freq_desc": "frequency",
    "reference_period_desc": "period",
    "Value": "value",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and read its data.
    snap = paths.load_snapshot("us_laying_hens_and_chicks.csv")
    tb = snap.read_csv(thousands=",", low_memory=False)

    #
    # Process data.
    #
    # Sanity check.
    error = "Data does not have the expected characteristics."
    assert tb[
        ["source_desc", "commodity_desc", "agg_level_desc", "state_name", "domain_desc", "unit_desc"]
    ].drop_duplicates().values.tolist() == [["SURVEY", "CHICKENS", "NATIONAL", "US TOTAL", "TOTAL", "HEAD"]], error

    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Improve table format.
    tb = tb.format(["data_item", "year", "period"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()

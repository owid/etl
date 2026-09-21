"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from the raw data, and how to rename them.
COLUMNS = {
    "Country": "country",
    "Variable": "variable",
    "Year": "year",
    "Unit": "unit",
    "Symbol": "symbol",
    "Value": "value",
}

# Expected data quality symbols:
# A - Official value, E - Estimate, I - Imputed, W - Wapor derived, X - External value.
SYMBOLS_EXPECTED = {"A", "E", "I", "W", "X"}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("aquastat.zip")

    # The archive contains a single long-format CSV, confusingly named "zip".
    with snap.extracted() as archive:
        tb = archive.read("zip", force_extension="csv")

    #
    # Process data.
    #
    # Select and rename columns.
    # NOTE: "M49" (numeric country code) and "Symbol Description" (a fixed one-to-one mapping of "Symbol") are dropped.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Sanity checks.
    assert set(tb["symbol"]) == SYMBOLS_EXPECTED, "Data quality symbols changed; check their descriptions."
    assert tb["value"].notnull().all(), "Unexpected missing values."
    # NOTE: Some variables (e.g. dimensionless indexes like the Human Development Index) have no unit at all.
    assert tb.groupby("variable", observed=True)["unit"].nunique().le(1).all(), "Variables with multiple units."

    # A few country-variable-years appear twice, with identical values but different symbols (e.g. once as official
    # and once as estimate). Check that duplicated rows never carry conflicting values, and keep the first row of
    # each group, preferring official values (symbols happen to be alphabetically ordered by quality: A, E, I, W, X).
    assert tb.groupby(["country", "variable", "year"], observed=True)["value"].nunique().eq(1).all(), (
        "Rows duplicated in country-variable-year carry conflicting values."
    )
    tb = tb.sort_values(["country", "variable", "year", "symbol"]).drop_duplicates(
        subset=["country", "variable", "year"], keep="first"
    )

    # Use categoricals for low-cardinality string columns.
    for column in ["country", "variable", "unit", "symbol"]:
        tb[column] = tb[column].astype("category")

    # Improve table format.
    tb = tb.format(["country", "variable", "year"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

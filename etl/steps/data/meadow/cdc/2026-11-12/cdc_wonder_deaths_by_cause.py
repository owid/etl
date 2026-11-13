"""Load a snapshot and create a meadow dataset.

The snapshot has one row per year and cause (NCHS's list of 113 selected causes, plus all causes), as returned by
CDC WONDER. This step turns the numbers into numeric columns: WONDER writes thousands separators, and marks rates
based on fewer than 20 deaths as "Unreliable" and hidden counts as "Suppressed", which become missing values.
"""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Markers WONDER uses instead of a number.
MISSING_MARKERS = {"Unreliable", "Suppressed", "Not Applicable"}


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("cdc_wonder_deaths_by_cause.csv")
    tb = snap.read_csv(dtype=str)

    #
    # Process data.
    #
    # Check that only known markers stand in for numbers.
    for column in ["deaths", "population", "crude_rate"]:
        non_numeric = set(tb.loc[~tb[column].str.fullmatch(r"[\d,]+(\.\d+)?"), column])
        assert non_numeric <= MISSING_MARKERS, f"Unexpected values in {column}: {non_numeric - MISSING_MARKERS}"

    # Convert numbers, turning WONDER's markers into missing values.
    for column in ["deaths", "population", "crude_rate"]:
        tb[column] = pr.to_numeric(tb[column].str.replace(",", "", regex=False), errors="coerce")
    tb["year"] = tb["year"].astype(int)
    tb["cause"] = tb["cause"].astype("category")
    tb["database"] = tb["database"].astype("category")

    tb = tb.format(["cause", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()

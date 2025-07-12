"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("trade.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Drop unnecessary columns.
    tb = tb.drop(columns=["DATASET", "SERIES_CODE", "OBS_MEASURE", "FREQUENCY", "SCALE"])

    # Melt year columns (1948-2024) into rows
    year_columns = [str(year) for year in range(1948, 2025)]
    tb = tb.melt(
        id_vars=["COUNTRY", "INDICATOR", "COUNTERPART_COUNTRY"],
        value_vars=year_columns,
        var_name="year",
        value_name="value",
    )

    # Convert year column to integer
    tb["year"] = tb["year"].astype(int)

    tables = [tb.format(["country", "year", "indicator", "counterpart_country"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

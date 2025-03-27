"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to keep and their new names
COLUMNS_TO_KEEP = {
    "Reference area": "country",
    "TIME_PERIOD": "year",
    "Measure": "indicator",
    "Unit of measure": "unit",
    "OBS_VALUE": "value",
    "Observation status": "status",
    "Unit multiplier": "unit_multiplier",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_public_finance = paths.load_snapshot("govt_glance_public_finance.csv")
    snap_size_public_procurement = paths.load_snapshot("govt_glance_size_public_procurement.csv")

    # Load data from snapshot.
    tb_public_finance = snap_public_finance.read()
    tb_size_public_procurement = snap_size_public_procurement.read()

    #
    # Process data.
    #
    tb_public_finance = filter_columns_and_format(
        tb=tb_public_finance,
        short_name="public_finance",
    )
    tb_size_public_procurement = filter_columns_and_format(
        tb=tb_size_public_procurement,
        short_name="size_public_procurement",
    )

    # Define tables list
    tables = [tb_public_finance, tb_size_public_procurement]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap_public_finance.metadata)

    # Save meadow dataset.
    ds_meadow.save()


def filter_columns_and_format(tb: Table, short_name: str) -> Table:
    """
    Filter columns and format tables.
    """

    tb = tb[COLUMNS_TO_KEEP.keys()]

    # Rename columns.
    tb = tb.rename(columns=COLUMNS_TO_KEEP)

    # Drop status column.
    tb = tb.drop(columns=["status"])

    # Format tables
    tb = tb.format(["country", "year", "indicator", "unit"], short_name=short_name)

    return tb

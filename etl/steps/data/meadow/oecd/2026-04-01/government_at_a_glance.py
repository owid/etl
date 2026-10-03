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

# Define index columns
INDEX_COLUMNS = ["country", "year", "indicator", "unit"]


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_public_finance = paths.load_snapshot("govt_glance_public_finance.csv")
    snap_size_public_procurement = paths.load_snapshot("govt_glance_size_public_procurement.csv")
    snap_public_finance_economic_transaction = paths.load_snapshot(
        "govt_glance_public_finance_economic_transaction.csv"
    )
    snap_public_finance_by_function = paths.load_snapshot("govt_glance_public_finance_by_function.csv")

    # Load data from snapshot.
    tb_public_finance = snap_public_finance.read()
    tb_size_public_procurement = snap_size_public_procurement.read()
    tb_public_finance_economic_transaction = snap_public_finance_economic_transaction.read()
    tb_public_finance_by_function = snap_public_finance_by_function.read()

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
    tb_public_finance_economic_transaction = filter_columns_and_format(
        tb=tb_public_finance_economic_transaction,
        short_name="public_finance_economic_transaction",
    )
    tb_public_finance_by_function = filter_columns_and_format(
        tb=tb_public_finance_by_function,
        short_name="public_finance_by_function",
    )

    # Define tables list
    tables = [
        tb_public_finance,
        tb_size_public_procurement,
        tb_public_finance_economic_transaction,
        tb_public_finance_by_function,
    ]

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

    # Define columns to keep
    columns_to_keep = COLUMNS_TO_KEEP.copy()
    index_columns = INDEX_COLUMNS.copy()

    if short_name == "public_finance_economic_transaction":
        # Add {"Economic transaction": "economic_transaction"} to columns_to_keep
        columns_to_keep["Economic transaction"] = "economic_transaction"

        # Add "economic_transaction" to index_columns
        index_columns.append("economic_transaction")

        # Add " by economic transaction" to each row in Measure column
        tb["Measure"] = tb["Measure"].apply(lambda x: f"{x} by economic transaction")

    elif short_name == "public_finance_by_function":
        # Add {"Expenditure": "function"} to columns_to_keep
        columns_to_keep["Expenditure"] = "function"

        # Add "function" to index_columns
        index_columns.append("function")

        # Add " by function" to each row in Measure column
        tb["Measure"] = tb["Measure"].apply(lambda x: f"{x} by function")

    # Keep only columns in columns_to_keep
    tb = tb[columns_to_keep.keys()]

    # Rename columns.
    tb = tb.rename(columns=columns_to_keep, errors="raise")

    # Drop status column.
    tb = tb.drop(columns=["status"], errors="raise")

    # Format tables
    tb = tb.format(index_columns, short_name=short_name)

    return tb

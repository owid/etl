"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("vaccine_confidence.xlsx")

    # Load data from each sheet in the snapshot
    tb_imp_children = snap.read(sheet_name="ImpChildren")
    tb_imp_children["question"] = "ImpChildren"
    tb_safe = snap.read(sheet_name="Safe")
    tb_safe["question"] = "Safe"
    tb_effective = snap.read(sheet_name="Effective")
    tb_effective["question"] = "Effective"
    tb_beliefs = snap.read(sheet_name="Beliefs")
    tb_beliefs["question"] = "Beliefs"

    tables = []
    for table in [tb_imp_children, tb_safe, tb_effective, tb_beliefs]:
        short_name = table["question"].iloc[0].lower()
        # Remove duplicate rows - there are some rows that are identical except for the 'Set' column
        table = remove_duplicate_rows(table, short_name)
        tables.append(table)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()


def remove_duplicate_rows(tb, short_name: str):
    """
    Remove duplicate rows from the table - there are some rows that are identical except for the 'Set' column
    """
    cols = tb.columns.drop("Set").tolist()
    tb = tb.drop_duplicates(subset=cols)

    # Reset index after dropping duplicates
    tb = tb.reset_index(drop=True)

    tb = tb.format(["country", "year", "set"], short_name=short_name)
    return tb

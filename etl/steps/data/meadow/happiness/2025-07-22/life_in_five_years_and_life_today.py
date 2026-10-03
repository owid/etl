"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("life_in_five_years_and_life_today.xlsx")

    # Read data from snapshot.
    data = snap.ExcelFile()

    #
    # Process data.
    #
    # Load data from the two sheets.
    tb_today = data.parse(sheet_name="Life Today", skiprows=7)
    tb_5_years = data.parse(sheet_name="Life in Five Years  ", skiprows=7)

    # Remove empty columns.
    tb_today = tb_today.dropna(axis=1, how="all")
    tb_5_years = tb_5_years.dropna(axis=1, how="all")

    # Improve tables format.
    tables = [
        tb_today.format(["geography", "time"], short_name="life_satisfaction_today"),
        tb_5_years.format(["geography", "time"], short_name="life_satisfaction_in_5_years"),
    ]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

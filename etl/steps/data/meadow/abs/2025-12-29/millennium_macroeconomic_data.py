"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("millennium_macroeconomic_data.xlsx")

    # Load data from snapshot.
    tb_wages = snap.read(
        sheet_name="A47. Wages and prices",
        skiprows=5,
        usecols="A:B",
    )

    print(tb_wages)

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("corruption.xlsx")

    tb = snap.read(skiprows=2)
    #
    # Process data.
    #
    columns = ["Country", "Indicator", "Dimension", "Category", "Sex", "Age", "Year", "VALUE", "Unit of measurement"]
    tb = tb[columns]

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [tb.format(["country", "year", "indicator", "dimension", "category", "sex", "age", "unit_of_measurement"])]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save meadow dataset.

    ds_meadow.save()

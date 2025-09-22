"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("historic_exports.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="volume")

    #
    # Process data.
    #
    tb = tb[["Unnamed: 0", "Extrapolation 1815-1913"]]
    tb["country"] = "World"

    tb = tb.rename(
        columns={
            "Unnamed: 0": "year",
            "Extrapolation 1815-1913": "historic_trade",
        }
    )

    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

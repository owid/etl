"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("wto_trade_growth.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="Chart Volume")

    #
    # Process data.
    #
    tb = tb[["Unnamed: 0", "Volume (Total).1"]]
    tb["country"] = "World"

    tb = tb.rename(
        columns={
            "Unnamed: 0": "year",
            "Volume (Total).1": "volume_index",
        }
    )

    # Remove rows where year or volume_index is NaN
    tb = tb.dropna(subset=["year", "volume_index"])

    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

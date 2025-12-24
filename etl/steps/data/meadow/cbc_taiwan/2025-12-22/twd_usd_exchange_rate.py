"""Load a snapshot and create a meadow dataset for TWD/USD exchange rates."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Load snapshot and create meadow dataset."""
    # Load inputs.
    snap = paths.load_snapshot("twd_usd_exchange_rate.csv")

    # Read data from snapshot
    tb = snap.read()

    # Ensure date column is datetime
    tb["date"] = tb["date"].astype("datetime64[ns]")

    # Ensure exchange_rate is float
    tb["exchange_rate"] = tb["exchange_rate"].astype(float)

    # Add metadata origins
    tb["exchange_rate"].metadata.origins = [snap.metadata.origin]

    # Format table with proper index and short name
    tb = tb.format(["date"], short_name="twd_usd_exchange_rate")

    # Save outputs.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True)
    ds_meadow.save()

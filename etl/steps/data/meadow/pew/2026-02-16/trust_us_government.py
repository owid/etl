"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("trust_us_government.csv")

    # Load data from snapshot, skipping the title/subtitle/blank rows at the top.
    tb = snap.read(skiprows=3)

    #
    # Process data.
    #
    # Drop trailing empty rows and notes at the bottom.
    tb = tb.dropna(how="any").reset_index(drop=True)

    # Rename the unnamed source column.
    # NOTE: Check if this is needed in future updates
    tb = tb.rename(columns={c: "source" for c in tb.columns if c.startswith("Unnamed")})

    # Add country
    tb["country"] = "United States"

    # Improve tables format.
    tables = [tb.format(["country", "date", "source"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

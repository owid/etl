"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("summary_reporting_system_estimates.csv")

    # Load data from snapshot.
    tb = snap.read()
    tb.loc[tb["state_name"].isna(), "state_abbr"] = "US"
    tb.loc[tb["state_abbr"] == "US", "state_name"] = "United States"

    tb = tb[tb["state_name"] == "United States"]
    tb = tb.rename(columns={"state_name": "country"})
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

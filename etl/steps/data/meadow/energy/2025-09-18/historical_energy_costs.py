"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("historical_energy_costs.xlsx")

    # Load data from snapshot.
    data = snap.ExcelFile()

    #
    # Process data.
    #
    # Read sheet on cost per useful energy.
    tb = data.parse(sheet_name="useful_energy_costs", skiprows=10)

    # Improve tables format.
    tb = tb.format(["year"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

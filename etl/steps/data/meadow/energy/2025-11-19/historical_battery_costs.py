"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("historical_battery_costs.xlsx")

    # Load data from snapshot.
    data = snap.ExcelFile()

    #
    # Process data.
    #
    # Read sheet on cost per useful energy.
    # NOTE: The file contains many columns, potentially other interesting data. But for now, we will just load the ones needed for an experience curve for batteries.
    tb = data.parse(sheet_name="Batteries", skiprows=35)[
        [
            "Year",
            "Li-ion batteries All cells Price Global (Representative), USD(2024)/kWh",
            "Li-ion batteries All Li-ion batteries Cumulative production Global (Representative), GWh",
        ]
    ]

    # Improve tables format.
    tb = tb.format(["year"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

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
            # Price.
            "Li-ion batteries All cells Price Global (Representative), USD(2024)/kWh",
            "Li-ion batteries Cylindrical cells Price Global (Representative), USD(2024)/kWh",
            "Li-ion batteries EV battery pack Price Global (Representative), USD(2024)/kWh",
            "Li-ion batteries Utility-scale BESS Cost Global (Representative), USD(2024)/kWh",
            "Li-ion batteries Residential BESS Cost Germany (Representative), USD(2024)/kWh",
            # Annual production and annual additions.
            "Li-ion batteries All Li-ion batteries Annual production Global (Representative), GWh/yr",
            "Li-ion batteries EV batteries Annual additions Global (Representative), GWh/yr",
            "Li-ion batteries Utility-scale BESS Annual additions Global (Representative), GWh/yr",
            # Cumulative production.
            # NOTE: In Rupert's data the EV and BESS cumulative series are in GWh/yr, but they are cumulative stocks, so I understand they should be in GWh.
            "Li-ion batteries All Li-ion batteries Cumulative production Global (Representative), GWh",
            "Li-ion batteries EV batteries Cumulative additions Global (Representative), GWh/yr",
            "Li-ion batteries Utility-scale BESS Cumulative additions Global (Representative), GWh/yr",
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

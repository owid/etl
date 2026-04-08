"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    snapshot_names = ["growth_rate_productivity.csv", "growth_rate_gdp_pc.csv"]
    tables = []
    for snapshot_name in snapshot_names:
        # Retrieve snapshot.
        snap = paths.load_snapshot(snapshot_name)

        # Load data from snapshot.
        tb = snap.read()

        #
        # Process data.
        #

        # Rename columns.
        tb = tb.rename(
            columns={
                "Entity": "country",
                "Year": "year",
            },
            errors="raise",
        )

        if snapshot_name == "growth_rate_productivity.csv":
            tb = tb.rename(
                columns={"Growth Rate of Labor Productivity": "growth_rate_productivity"},
                errors="raise",
            )
        else:
            tb = tb.rename(
                columns={"Growth Rate of GDP per capita": "growth_rate_gdp_pc"},
                errors="raise",
            )
        # Improve table format.
        tb = tb.format(["country", "year"], short_name=snapshot_name.replace(".csv", ""))

        # Append current table to list of tables.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables)

    # Save meadow dataset.
    ds_meadow.save()

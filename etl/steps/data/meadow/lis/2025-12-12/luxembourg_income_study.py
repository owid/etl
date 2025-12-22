"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to keep and their new names
COLUMNS_TO_KEEP = {
    "cname": "country",
    "year": "year",
    "indicator": "indicator",
    "variable": "welfare_type",
    "equiv": "equivalence_scale",
    "value": "value",
}


def run() -> None:
    #
    # Load inputs.
    #
    snapshot_names = ["lis_incomes.csv", "lis_absolute_poverty.csv", "lis_inequality.csv", "lis_relative_poverty.csv"]
    tables = []
    for snapshot_name in snapshot_names:
        # Retrieve snapshot.
        snap = paths.load_snapshot(
            snapshot_name,
        )

        # Load data from snapshot.
        tb = snap.read()

        #
        # Process data.
        #

        # Keep only relevant columns and rename them.
        tb = tb[list(COLUMNS_TO_KEEP.keys())].rename(columns=COLUMNS_TO_KEEP, errors="raise")

        # Improve table format.
        tb = tb.format(
            ["country", "year", "indicator", "welfare_type", "equivalence_scale"],
            short_name=snapshot_name.replace(".csv", "").replace("lis_", ""),
        )

        # Append current table to list of tables.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables)

    # Save meadow dataset.
    ds_meadow.save()

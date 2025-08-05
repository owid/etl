"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("health_expenditure.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    tb = keep_relevant_columns(tb)
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "indicator", "financing_scheme"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def keep_relevant_columns(tb: Table) -> Table:
    """
    Keep only the columns that are needed and rename them.
    """

    # Keep only the columns that are needed
    cols_to_keep = [
        "Reference area",
        "Unit of measure",
        "Financing scheme",
        "TIME_PERIOD",
        "OBS_VALUE",
        "Observation status",
        "Observation status 2",
        "Observation status 3",
    ]
    tb = tb[cols_to_keep]

    # Rename columns
    tb = tb.rename(
        columns={
            "Reference area": "country",
            "TIME_PERIOD": "year",
            "OBS_VALUE": "value",
            "Unit of measure": "indicator",
        }
    )

    return tb

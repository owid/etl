"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("cder_approvals.csv")

    # Load data from snapshot.
    tb = snap.read()

    # drop columns:
    tb = tb.drop(
        columns=[
            "Dosage Form(1)",
            "Route of Administration(1)",
            "Dosage Form(2)",
            "Route of Administration(2)",
            "Dosage Form(3)",
            "Route of Administration(3)",
        ],
        errors="raise",
    )

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["application_number__1", "application_number__2", "application_number__3"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

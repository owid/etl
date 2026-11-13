"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("sst.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Only keep the nino4 anomaly
    tb = tb[["year", "month", "nino4_anomaly", "nino3_4_anomaly", "oni_anomaly"]]
    tb["country"] = "World"

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [tb.format(["country", "month", "year"])]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("global_health_mpox.csv")

    # Load data from snapshot.
    tb = snap.read(low_memory=False)
    tb = tb[["ID", "Case_status", "Location_Admin0", "Date_report_source_I"]]
    assert all(tb["Date_report_source_I"].notna())

    tb = tb.rename(columns={"Date_report_source_I": "date", "Location_Admin0": "country"})
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    # Row per individual - will aggregate in garden step so will keep ID as index for now
    tb = tb.format(["id", "country", "date"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

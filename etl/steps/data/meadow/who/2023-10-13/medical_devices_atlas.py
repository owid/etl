"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    snaps = [paths.load_snapshot(f"medical_devices_atlas_{devices}.csv") for devices in ["mri", "ct", "pet", "gc_nm"]]

    # Default metadata (metadata is common to all snapshots)
    snap_metadata = snaps[0].metadata

    # Load data from snapshot.
    tbs = [snap.read_csv(encoding="latin-1") for snap in snaps]  # Read all snapshots
    tb = pr.concat(tbs, ignore_index=True, short_name=paths.short_name)  # Concatenate all snapshots into one table

    #
    # Process data.
    #
    tb = tb[["Indicator", "Location", "Period", "Value"]]
    tb = tb.rename(columns={"Location": "country", "Period": "year", "Value": "value"})

    tb.loc[
        tb["year"] == "2017-2019", "year"
    ] = "2018"  # 2017-2019 is the average of 2017, 2018 and 2019 so we replace it with 2018 to be consistent with the other years
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year", "indicator"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap_metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

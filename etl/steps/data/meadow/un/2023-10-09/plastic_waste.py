"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snaps = [paths.load_snapshot(f"plastic_waste_{years}.csv") for years in ["1987_1998", "1999_2010", "2011_2022"]]

    # Default metadata (metadata is common to all snapshots)
    snap_metadata = snaps[0].metadata

    # Load data from snapshot.
    tbs = [snap.read_csv(encoding="latin-1") for snap in snaps]
    tb = pr.concat(tbs, ignore_index=True, short_name=paths.short_name)

    #   Check that there is, at most, one entry per combination of:
    #    - Year ("RefYear")
    #    - Type of flow (export/import, denoted by "FlowDesc")
    #    - Reporting country ("ReporterDesc")
    #    - Partner country ("PartnerDesc")
    #    - Mode of transport ("MotDesc")
    assert (
        tb.groupby(["RefYear", "FlowDesc", "ReporterDesc", "PartnerDesc", "MotDesc"]).size().max() == 1
    ), "There should be, at most, one entry per year, export type, origin country, destination country and mode of transport combination"
    #
    # Process data.
    #
    tb = tb.set_index(["RefYear", "FlowDesc", "ReporterDesc", "PartnerDesc", "MotDesc"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap_metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.

    # Retrieve snapshots.
    snap = paths.load_snapshot("equaldex.csv")
    snap_current = paths.load_snapshot("equaldex_current.csv")
    snap_indices = paths.load_snapshot("equaldex_indices.csv")

    # Load data from snapshots.
    tb = snap.read(safe_types=False)
    tb_current = snap_current.read()
    tb_indices = snap_indices.read()

    #
    # Process data.
    # CURRENT DATASET

    # Rename year_extraction to year
    tb_current = tb_current.rename(columns={"year_extraction": "year"})

    # Set index as country, year and issue and verify that there are no duplicates
    tb_current = tb_current.format(["country", "year", "issue"])

    # COMPLETE DATASET
    # Drop duplicates in the index
    # Equaldex collaborators are cleaning these duplicates, that sometimes provide different values for the same year
    # We are keeping the first value for each year
    tb = tb[~tb.duplicated(subset=["country", "year", "issue"], keep="first")]

    # Remove date_modified  and dataset columns
    tb = tb.drop(columns=["date_modified", "dataset"])

    # Set index as country, year and issue and verify that there are no duplicates
    tb = tb.format(["country", "year", "issue"])

    # INDICES DATASET
    # Rename name as country
    tb_indices = tb_indices.rename(columns={"name": "country"})
    # Set index as country and year and verify that there are no duplicates
    tb_indices = tb_indices.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb, tb_current, tb_indices], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new garden dataset.
    ds_meadow.save()

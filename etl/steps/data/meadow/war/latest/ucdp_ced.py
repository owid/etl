"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    paths.log.info("start")

    #
    # Load inputs.
    #
    # Retrieve snapshot: current year (latest quarterly release).
    # NOTE: 2025 preliminary data is no longer needed — the stable 26.1 release covers it.
    snap_curr = paths.load_snapshot(short_name="ucdp_ced_v26_01_26_03.csv")

    # Read as table
    tb = snap_curr.read_csv()
    tb = tb.drop_duplicates()

    # Sanity check
    assert tb["id"].is_unique, "Event IDs should be unique in the CED snapshot!"

    # Format table
    tb = tb.format(
        "id",
        short_name="ucdp_ced",
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=[tb],
        check_variables_metadata=True,
    )  # ty: ignore

    # Save changes in the new garden dataset.
    ds_meadow.save()

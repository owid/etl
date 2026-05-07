"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    paths.log.info("start")

    #
    # Load inputs.
    #
    # Retrieve snapshots: previous year (full preliminary) + current year (latest quarterly release).
    snap_prev = paths.load_snapshot(short_name="ucdp_ced_v25_01_25_12.csv")
    snap_curr = paths.load_snapshot(short_name="ucdp_ced_v26_01_26_03.csv")

    # Read as tables
    tb_prev = snap_prev.read_csv()
    tb_curr = snap_curr.read_csv()

    # Sanity checks
    assert (tb_prev.columns == tb_curr.columns).all(), "Columns do not match between quarterly snapshots!"

    # Combine tables
    tb = pr.concat([tb_prev, tb_curr], ignore_index=True)
    tb = tb.drop_duplicates()

    # Each quarterly release only contains events for its own preview year, so IDs should be unique across them.
    assert tb["id"].is_unique, "Event IDs should be unique across the combined CED snapshots!"

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

"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    paths.log.info("start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_10 = paths.load_snapshot(short_name="ucdp_ced_v24_0_10.csv")
    snap_q3 = paths.load_snapshot(short_name="ucdp_ced_v24_01_24_09.csv")

    # Read as tables
    tb_10 = snap_10.read_csv()
    tb_q3 = snap_q3.read_csv()

    # Remove spurious columns, sanity checks
    if "#" in tb_10.columns:
        tb_10 = tb_10.drop(columns=["#"])

    assert (tb_10.columns == tb_q3.columns).all(), "Columns do not match between monthly and quarterly snapshots!"

    # Combine tables
    tb = pr.concat([tb_q3, tb_10], ignore_index=True)
    tb = tb.drop_duplicates()

    # Monthly data may have events that were already reported in the quarterly release.
    # Idea: Check that this is the case, and safely remove duplicates from the quarterly release, since the monthly release is more up-to-date.

    ## Ensure that all duplicate IDs are indeed because of duplicates between monthly-quarterly
    value_counts = tb["id"].value_counts()
    assert set(value_counts.unique()) == {1, 2}, "IDs should appear once or twice, not more!"
    ids_duplicated = list(value_counts[value_counts > 1].index)
    assert len(ids_duplicated) == tb_10[tb_10["id"].isin(ids_duplicated)].shape[0], "All duplicated ID"
    tb = tb.drop_duplicates(subset="id", keep="last")

    # Format table
    tb = tb.format("id")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
    )  # type: ignore

    # Save changes in the new garden dataset.
    ds_meadow.save()

    paths.log.info("end")

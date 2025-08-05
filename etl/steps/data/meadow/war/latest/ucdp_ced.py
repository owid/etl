"""Load a snapshot and create a meadow dataset."""

# import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    paths.log.info("start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_ced = paths.load_snapshot(short_name="ucdp_ced_v25_01_25_06.csv")

    # Read as tables
    tb_ced = snap_ced.read_csv()

    # Use the following code if you need to append additional releases (e.g. monthly or quarterly releases)
    # # Remove spurious columns, sanity checks
    # if "#" in tb_aux.columns:
    #     tb_aux = tb_aux.drop(columns=["#"])

    # assert (tb_aux.columns == tb_24.columns).all(), "Columns do not match between monthly and quarterly snapshots!"

    # # Combine tables
    # tb = pr.concat([tb_24, tb_aux], ignore_index=True)
    # tb = tb.drop_duplicates()

    # Monthly data may have events that were already reported in the quarterly release.
    # Idea: Check that this is the case, and safely remove duplicates from the quarterly release, since the monthly release is more up-to-date.

    ## Ensure that all duplicate IDs are indeed because of duplicates between monthly-quarterly
    # value_counts = tb["id"].value_counts()
    # assert set(value_counts.unique()) == {1, 2}, "IDs should appear once or twice, not more!"
    # ids_duplicated = list(value_counts[value_counts > 1].index)
    # assert len(ids_duplicated) == tb_10[tb_10["id"].isin(ids_duplicated)].shape[0], "All duplicated ID"
    # tb = tb.drop_duplicates(subset="id", keep="last")

    # Comment the two lines below if you are appending additional releases
    tb = tb_ced.copy()
    tb = tb.drop_duplicates()

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
    )  # type: ignore

    # Save changes in the new garden dataset.
    ds_meadow.save()

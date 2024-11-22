"""Load a snapshot and create a meadow dataset."""

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

    # Check shapes

    tb = tb_10.format("id")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata, check_variables_metadata=True)  # type: ignore

    # Save changes in the new garden dataset.
    ds_meadow.save()

    paths.log.info("end")

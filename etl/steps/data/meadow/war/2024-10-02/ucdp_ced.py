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
    snap = paths.load_snapshot(short_name="ucdp_ced.csv")
    tb = snap.read_csv()
    tb = tb.format("id")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata, check_variables_metadata=True)  # type: ignore

    # Save changes in the new garden dataset.
    ds_meadow.save()

    paths.log.info("end")

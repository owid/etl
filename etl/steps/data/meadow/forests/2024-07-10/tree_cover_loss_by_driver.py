"""Load a snapshot and create a meadow dataset."""

from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("tree_cover_loss_by_driver.csv")
    tb = snap.read(usecols=["country", "year", "category", "area"])
    # Some large countries are broken down into smaller regions in the dataset, so we need to aggregate them here
    tb = tb.groupby(["country", "year", "category"]).sum().reset_index()
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "category"], short_name=paths.short_name)
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
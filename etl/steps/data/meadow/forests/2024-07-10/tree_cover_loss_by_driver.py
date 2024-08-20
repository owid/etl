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
    snap = paths.load_snapshot("dominant_driver.xlsx")
    tb = snap.read(sheet_name="data")
    tb = tb.drop(columns=["iso"])
    tb = tb.rename(columns={"loss year": "year", "Driver of loss": "category", "Tree cover loss (ha)": "area"})
    # Some large countries are broken down into smaller regions in the dataset, so we need to aggregate them here
    # tb = tb.groupby(["country", "loss year", "Driver of loss"]).sum().reset_index()
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "category"], short_name=paths.short_name)
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

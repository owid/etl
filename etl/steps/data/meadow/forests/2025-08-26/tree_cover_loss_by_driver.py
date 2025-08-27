"""Load a snapshot and create a meadow dataset."""

from structlog import get_logger

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("dominant_driver.xlsx")
    tb = snap.read(safe_types=False, sheet_name="tree_cover_loss_by_driver_iso")
    tb = tb.drop(columns=['Total 2001-2024'])
    tb = tb.melt(
        id_vars=['iso', 'Driver'],
        value_vars=[col for col in tb.columns if col not in ['iso', 'Driver']],  # year columns
        var_name='year',
        value_name='tree_cover_loss_ha'
    )
    tb = tb.rename(columns={'iso': 'country', 'Driver': 'category'})

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "category"], short_name=paths.short_name)
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset( tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

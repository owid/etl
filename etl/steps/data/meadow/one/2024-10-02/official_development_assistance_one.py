"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to keep
COLUMNS_TO_KEEP_SECTORS = [
    "year",
    "donor_name",
    "recipient_name",
    "sector_name",
    "value",
]

COLUMNS_TO_KEEP_CHANNELS = [
    "year",
    "donor_name",
    "recipient_name",
    "channel_code",
    "value",
]

# Define index columns (COLUMS_TO_KEEP minus value)
INDEX_COLUMNS_SECTORS = [col for col in COLUMNS_TO_KEEP_SECTORS if col != "value"]
INDEX_COLUMNS_CHANNELS = [col for col in COLUMNS_TO_KEEP_CHANNELS if col != "value"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("official_development_assistance_one.feather")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Make year integer.
    tb["year"] = tb["year"].astype(int)

    # Create tb_sectors and tb_channels, aggregating by the index columns.
    tb_sectors = tb.groupby(INDEX_COLUMNS_SECTORS, observed=True, dropna=False)["value"].sum().reset_index()
    tb_channels = tb.groupby(INDEX_COLUMNS_CHANNELS, observed=True, dropna=False)["value"].sum().reset_index()

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb_sectors = tb_sectors.format(INDEX_COLUMNS_SECTORS, short_name="sectors")
    tb_channels = tb_channels.format(INDEX_COLUMNS_CHANNELS, short_name="channels")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb_sectors, tb_channels], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()

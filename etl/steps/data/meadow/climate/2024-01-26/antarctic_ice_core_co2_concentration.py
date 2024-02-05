"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and load data.
    snap = paths.load_snapshot("antarctic_ice_core_co2_concentration.xls")
    tb = snap.read(sheet_name="CO2 Composite", skiprows=14)

    #
    # Process data.
    #
    # Ensure all columns are snake-case (and remove spurious spaces), set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["gasage__yr_bp"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)
    ds_meadow.save()

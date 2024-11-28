"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("polio_historical.xls")

    # Load data from snapshot.
    tb = snap.read(safe_types=False, sheet_name="Polio")

    #
    # Process data.
    #
    tb = pr.melt(tb, id_vars=["WHO_REGION", "ISO_code", "Cname", "Disease"], var_name="year", value_name="cases")
    tb = tb.drop(columns=["WHO_REGION", "ISO_code", "Disease"], errors="raise")
    tb = tb.rename(columns={"Cname": "country"}, errors="raise")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

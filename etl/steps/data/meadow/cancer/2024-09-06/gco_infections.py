"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("gco_infections.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Drop unnecessary columns.
    tb = tb.drop(columns=["id", "code", "ncases_sites", "ncases_all", "ir_att", "ir", "asr"])

    tb["sex"] = tb["sex"].astype(str).replace({"0": "both", "1": "males", "2": "females"})

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "sex", "agent", "cancer"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

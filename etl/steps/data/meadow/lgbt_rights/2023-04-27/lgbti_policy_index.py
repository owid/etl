"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("lgbti_policy_index.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="Sheet1")

    #
    # Process data.

    # Remove duplicated values for Australia in region = Europe (the author's original dataset has a duplicated value for Australia, one for Oceania and one for Europe).
    mask = (tb["country"] == "Australia") & (tb["region"] == "Europe")

    if mask.sum() > 0:
        paths.log.info("There are duplicated values for Australia in the Europe region. They will be removed.")
        tb = tb[~mask]

    #
    # Verify index and sort
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

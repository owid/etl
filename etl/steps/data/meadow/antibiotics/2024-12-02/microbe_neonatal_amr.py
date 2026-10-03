"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("microbe_neonatal_amr.csv")

    # Load data from snapshot.
    tb = snap.read()
    assert all(tb["Age"] == "Neonatal")
    assert len(tb["Counterfactual"].unique()) == 1
    assert all(tb["Infectious syndrome"] == "Bloodstream infections")
    #
    # Process data.
    #
    tb = tb.drop(columns=["Age", "Sex", "Measure", "Metric", "Infectious syndrome", "Pathogen Type", "Counterfactual"])
    tb = tb.rename(columns={"Location": "country", "Year": "year"})

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "pathogen"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

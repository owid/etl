"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("microbe_total_pathogens_amr.csv")

    # Load data from snapshot.
    tb = snap.read()
    assert all(tb["Age"] == "All Ages")
    assert all(tb["Sex"] == "Both sexes")
    assert all(tb["Measure"] == "Deaths")
    assert all(tb["Metric"] == "Number")
    assert all(tb["Counterfactual"] == "Attributable")
    assert all(tb["Infectious syndrome"] == "All infectious syndromes")

    #
    # Process data.
    tb = tb.drop(columns=["Age", "Sex", "Measure", "Metric", "Infectious syndrome", "Counterfactual"])
    tb = tb.rename(columns={"Location": "country", "Year": "year", "Pathogen": "pathogen"})
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "pathogen"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

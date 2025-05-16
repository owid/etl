"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and read its data.
    snap = paths.load_snapshot("nuclear_weapons_tests.csv")
    # Load data as string to be able to fix annotations issue (see below).
    tb = snap.read(dtype=str)

    #
    # Process data.
    #
    # Manually remove spurious numbers which are annotations.
    # Currently there is only one for North Korea in 2010.
    error = "Expected annotation for North Korea in 2010. It may have changed, so remove this part of the code."
    assert tb[(tb["Year"] == "2010")]["North Korea"].item() == "03", error
    # Remove the annotation.
    tb.loc[(tb["Year"] == "2010"), "North Korea"] = "0"

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()

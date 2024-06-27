"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("inequality_france.xlsx")

    # Load data from snapshot.
    tb = snap.read(header=2)

    #
    # Process data.
    # Make column names strings
    tb.columns = tb.columns.astype(str)

    # Make table long
    tb = tb.melt(id_vars=["Indicateur"], var_name="year", value_name="value")

    # Rename Indicateur to indicator
    tb = tb.rename(columns={"Indicateur": "indicator"})

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["indicator", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

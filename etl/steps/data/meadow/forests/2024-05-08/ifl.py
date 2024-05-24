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
    snap = paths.load_snapshot("ifl.xlsx")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    tb = pr.melt(tb, ["Name"], var_name="year_unit", value_name="ifl_area")
    tb["year"] = tb["year_unit"].str.extract("(\d{4})")[0]  # type: ignore
    tb = tb.drop(columns=["year_unit"])
    tb = tb.rename(columns={"Name": "country"})
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

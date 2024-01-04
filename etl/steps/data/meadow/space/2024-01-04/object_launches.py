"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("object_launches.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    tb = tb[["object.launch.stateOfRegistry_s1", "object.launch.dateOfLaunch_s1"]].rename(
        columns={
            "object.launch.stateOfRegistry_s1": "country",
            "object.launch.dateOfLaunch_s1": "year",
        }
    )
    tb["year"] = tb.year.str.slice(0, 4)

    # Add the number of launches for each country and year (and add metadata to the new column).
    tb = tb.groupby(["country", "year"], as_index=False).size().rename(columns={"size": "annual_launches"})
    tb["annual_launches"] = tb["annual_launches"].copy_metadata(tb["country"])

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

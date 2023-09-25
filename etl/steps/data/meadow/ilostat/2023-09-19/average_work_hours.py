"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("average_work_hours.csv")
    tb = snap.read(low_memory=True)
    tb.rename(
        columns={
            "ref_area.label": "country",
            "time": "year",
            "sex.label": "sex",
            "classif1.label": "economic_activity",
            "classif2.label": "education",
            "obs_value": "average_work_hours",
        },
        inplace=True,
    )
    tb = tb[["country", "year", "sex", "economic_activity", "education", "average_work_hours"]]

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.set_index(["country", "year", "sex", "economic_activity", "education"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

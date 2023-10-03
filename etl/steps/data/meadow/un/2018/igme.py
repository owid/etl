"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("igme.csv")

    # Load data from snapshot.
    tb = snap.read()
    tb = tb.rename(columns={"REF_AREA_NAME": "country", "REF_DATE": "year"})
    columns_to_keep = [
        "country",
        "year",
        "INDICATOR_NAME",
        "SEX_NAME",
        "SERIES_NAME_NAME",
        "UNIT_MEASURE_NAME",
        "OBS_VALUE",
        "LOWER_BOUND",
        "UPPER_BOUND",
    ]
    tb = tb[columns_to_keep]
    tb = tb[tb["SERIES_NAME_NAME"] == "UN IGME estimate"]
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = (
        tb.underscore().set_index(["country", "year", "indicator_name", "sex_name"], verify_integrity=True).sort_index()
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

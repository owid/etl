"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("self_inflicted_injuries.csv")

    # Load data from snapshot.
    tb = snap.read(skiprows=5, index_col=False)

    #
    # Process data.
    #

    columns_to_keep = [
        "Country Name",
        "Year",
        "Sex",
        "Age Group",
        "Number",
        "Percentage of cause-specific deaths out of total deaths",
        "Age-standardized death rate per 100 000 standard population",
        "Death rate per 100 000 population",
    ]
    tb = tb[columns_to_keep]

    tb = tb.rename(columns={"Country Name": "country"})
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().format(["country", "year", "sex", "age_group"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

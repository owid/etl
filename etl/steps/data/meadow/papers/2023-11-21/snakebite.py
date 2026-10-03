"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
YEAR = 2019


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("snakebite.xlsx")

    # Load data from snapshot.
    tb = snap.read(skiprows=1)
    tb["year"] = YEAR
    tb = tb.rename(
        columns={
            "Location": "country",
            "Count, 2019": "deaths_count",
            "Age-standardized rate per 100,000, 2019": "age_standardized_death_rate_per_100000",
            "Percent change from 1990 to 2019": "percent_change_in_deaths_from_1990_to_2019",
            "Count, 2019.1": "ylls_count",
            "Age-standardized rate per 100,000, 2019.1": "age_standardized_yll_rate_per_100000",
            "Percent change from 1990 to 2019.1": "percent_change_in_ylls_from_1990_to_2019",
        }
    )
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("hospital.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    tb = tb.pivot(index=["entity", "iso_code", "date"], columns="indicator", values="value").reset_index()

    # Rename columns
    tb = tb.rename(
        columns={
            "entity": "country",
            "iso_code": "country_code",
            "Daily ICU occupancy": "daily_occupancy_icu",
            "Daily ICU occupancy per million": "daily_occupancy_icu_per_1m",
            "Daily hospital occupancy": "daily_occupancy_hosp",
            "Daily hospital occupancy per million": "daily_occupancy_hosp_per_1m",
            "Weekly new ICU admissions": "weekly_admissions_icu",
            "Weekly new ICU admissions per million": "weekly_admissions_icu_per_1m",
            "Weekly new hospital admissions": "weekly_admissions_hosp",
            "Weekly new hospital admissions per million": "weekly_admissions_hosp_per_1m",
        }
    )

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "country_code", "date"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

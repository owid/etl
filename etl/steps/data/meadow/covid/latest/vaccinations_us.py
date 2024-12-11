"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("vaccinations_us.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Combine columns
    # Some columns have changed their name over time. Consolidate these under the same name.
    tb["Administered_Dose1_Recip"] = tb["Administered_Dose1_Recip"].fillna(tb["Administered_Dose1"])
    tb["Series_Complete_Yes"] = tb["Series_Complete_Yes"].fillna(
        tb["Administered_Dose2_Recip"].fillna(tb["Administered_Dose2"])
    )

    # Keep relevant columns and rename these
    columns_rename = {
        "Doses_Distributed": "total_distributed",
        "Doses_Administered": "total_vaccinations",
        "Administered_Dose1_Recip": "people_vaccinated",
        "Series_Complete_Yes": "people_fully_vaccinated",
        "additional_doses": "total_boosters",
        "Second_Booster": "total_boosters_2",
        "Bivalent_Booster": "total_boosters_biv",
        "Series_Complete_Janssen": "single_shots",
        "Date": "date",
        "LongName": "state",
        "Census2019": "census_2019",
    }
    tb = tb.loc[:, list(columns_rename.keys())].rename(columns=columns_rename)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["state", "date"])
    tb = tb.astype("Int64")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("polio_vaccine_schedule.xlsx")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Drop the last row, which contains export infor
    tb = tb.dropna(subset=["COUNTRYNAME"]).reset_index(drop=True)

    # Rename columns
    tb = tb.rename(columns={"COUNTRYNAME": "country", "YEAR": "year"})
    tb = (
        tb[
            [
                "country",
                "year",
                "SCHEDULERCODE",
            ]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    tb = tb.pivot(index=["country", "year"], columns=["SCHEDULERCODE"], values="SCHEDULERCODE")
    tb.columns = ["SCHEDULERCODE_" + str(col) for col in tb.columns]
    tb = tb.reset_index()
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format()
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

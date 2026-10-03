"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define original columns and their new names
COLUMNS = {
    "Libellé": "year",
    "Rapport interdécile D9/D5 du salaire net annuel en équivalent temps plein dans le secteur privé - Ensemble des salariés": "p90_p50_ratio",
    "Codes": "codes",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("interdecile_ratio.csv")

    # Load data from snapshot.
    tb = snap.read(sep=";")

    # Process data.

    # Rename columns
    tb = tb.rename(columns=COLUMNS)

    # Remove all rows in year that are not numeric
    tb = tb[tb["year"].str.isnumeric()].reset_index(drop=True)

    # Add country
    tb["country"] = "France"

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

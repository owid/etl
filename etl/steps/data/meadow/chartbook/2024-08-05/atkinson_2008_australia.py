"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("atkinson_2008_australia.xls")

    # Load data from snapshot.
    tb_oecd_lms = snap.read(sheet_name="Table A.3 (OECD LMS)", usecols="C:I", skiprows=2)

    #
    # Process data.
    #
    # Rename Unnamed:2 to year
    tb_oecd_lms = tb_oecd_lms.rename(columns={"Unnamed: 2": "year"})

    # Add country column
    tb_oecd_lms["country"] = "Australia"

    # Remove missing year columns
    tb_oecd_lms = tb_oecd_lms.dropna(subset=["year"])

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb_oecd_lms = tb_oecd_lms.format(["country", "year"], short_name="oecd_lms")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb_oecd_lms], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()

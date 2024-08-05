"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table

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
    tb_eeh = snap.read(sheet_name="Table A.5 (EEH)", usecols="C:I", skiprows=3)

    #
    # Process data.
    #
    # Rename Unnamed:2 to year
    tb_oecd_lms = rename_year_and_add_country(tb=tb_oecd_lms, short_name="oecd_lms")
    tb_eeh = rename_year_and_add_country(tb=tb_eeh, short_name="eeh")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb_oecd_lms, tb_eeh], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def rename_year_and_add_country(tb: Table, short_name: str) -> Table:
    """
    Rename the unnamed column to year and add a country column with the value Australia
    Also, remove missing year columns, create indices and add short_name
    """

    tb = tb.rename(columns={"Unnamed: 2": "year"})
    tb["country"] = "Australia"
    tb = tb.dropna(subset=["year"])

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"], short_name=short_name)

    return tb

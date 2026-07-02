"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and read data.
    tb = paths.read_snap_table("fur_laws")

    #
    # Process data.
    #
    # Normalize whitespace in country names (the spreadsheet cells contain stray spaces and line breaks).
    tb["COUNTRY"] = tb["COUNTRY"].astype("string").str.replace(r"\s+", " ", regex=True).str.strip()

    # Remove last row, which simply gives general additional information.
    # It says that any country that is not informed in the spreadsheet can be assumed to have no active fur farms.
    # We confirmed this assumption with Fur Free Alliance.
    error = "Spreadsheet has changed. Manually check this part of the code."
    assert tb.iloc[-1]["COUNTRY"] == "No active fur farms have been reported in all other countries", error
    tb = tb[:-1]

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    # NOTE: Do not verify integrity, since there are duplicated countries (to be fixed in the garden step).
    tb = tb.format(keys=["country"], verify_integrity=False)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True)
    ds_meadow.save()

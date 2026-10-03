"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("robot_density.xlsx")

    # Load data from snapshot.
    # The Excel file has a header row at index 1, actual data starts at index 2
    # Row 0 is the title, row 1 has column names, data starts at row 2
    tb = snap.read_excel(sheet_name="Tabelle1", skiprows=2, names=["country", "notes", "robot_density"])
    #
    # Process data.
    #
    # Drop the last row which contains source notes
    tb = tb[tb["country"].notna()].copy()

    # Remove any rows that don't contain actual country data
    tb = tb[tb["robot_density"].notna()].copy()
    tb = tb.drop(columns=["notes"])
    tb["year"] = 2023

    tb = tb.format(["country", "year"])
    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

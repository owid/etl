"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Relative poverty lines and their new names
POVERTY_LINES = {
    "Seuil à 40 %": "headcount_ratio_40_median",
    "Seuil à 50 %": "headcount_ratio_50_median",
    "Seuil à 60 %": "headcount_ratio_60_median",
    "Seuil à 70 %": "headcount_ratio_70_median",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("relative_poverty_france.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="Données", skiprows=3)

    #
    # Process data.
    tb = reformat_table(tb)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def reformat_table(tb: Table) -> Table:
    """
    Keep only rows with data, transpose and add country column.
    """

    tb = tb.copy()

    # In the first column, keep only rows starting with "Seuil"
    tb = tb[tb.iloc[:, 0].str.startswith("Seuil")]

    # Rename categories in the first column
    tb.iloc[:, 0] = tb.iloc[:, 0].map(POVERTY_LINES)

    # Set the first column as the index
    tb = tb.set_index(tb.columns[0])

    # Invert the table
    tb_transposed = tb.copy().T

    # Copy metadata from tb
    for col in tb_transposed.columns:
        tb_transposed[col] = tb_transposed[col].copy_metadata(tb[1975])

    # Reset index and rename first column to "year"
    tb_transposed = tb_transposed.reset_index()
    tb_transposed = tb_transposed.rename(columns={"index": "year"})
    tb_transposed["year"] = tb_transposed["year"].astype("string")

    # Add country
    tb_transposed["country"] = "France"

    return tb_transposed

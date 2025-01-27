"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("marriages.xlsx")

    # Load data from snapshot.
    # Load sheets on the proportions of men (14a) and women (14b) who had ever married by certain ages, for birth cohorts, England and Wales.
    tb_men = snap.read(sheet_name="14a")
    tb_women = snap.read(sheet_name="14b")

    #
    # Process data.
    #
    tables = []
    for tb, gender in zip([tb_men, tb_women], ["Men", "Women"]):
        # Find the row where the first column contains "Year of birth"
        header_row = tb[tb.iloc[:, 0].str.contains("Year of birth", na=False)].index[0]

        # Set the header row dynamically and drop rows before the header row
        tb.columns = tb.iloc[header_row]
        tb = tb.drop(index=range(header_row + 1)).reset_index(drop=True)

        # Melt the Table to create a 'year_of_birth' column
        tb = tb.melt(id_vars=["Year of birth"], var_name="age", value_name="cumulative_percentage_per_1000")

        # Keep only numbers in the age column
        tb["age"] = tb["age"].str.extract(r"(\d+)").astype(int)

        # Add gender column
        tb["gender"] = gender

        tb = tb.rename(columns={"Year of birth": "year"})
        tables.append(tb)
    tb = pr.concat(tables)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["year", "age", "gender"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()

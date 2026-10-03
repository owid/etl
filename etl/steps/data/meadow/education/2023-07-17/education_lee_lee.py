"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("education_lee_lee.xlsx")
    # Load data from snapshot.
    tb = snap.read_excel()
    #
    # Process data.
    #
    # Define a dictionary for renaming columns to have more informative names and making sure they are consistent with the 'projections' dataset columns.
    COLUMNS_RENAME = {
        "No Schooling": "Percentage of no education",
        "Primary, total": "Percentage of primary education",
        "Primary, completed": "Percentage of complete primary education attained",
        "Secondary, total": "Percentage of secondary education",
        "Secondary, completed": "Percentage of complete secondary education attained",
        "Tertiary, total": "Percentage of tertiary education",
        "Tertiary, completed": "Percentage of complete tertiary education attained",
        "Avg. Years of Total Schooling": "Average years of education",
        "Avg. Years of Primary Schooling": "Average years of primary education",
        "Avg. Years of Secondary Schooling": "Average years of secondary education",
        "Avg. Years of Tertiary\n Schooling": "Average years of tertiary education",
        "Population\n(1000s)": "Population (thousands)",
        "Primary": "Primary enrollment rates",
        "Secondary": "Secondary enrollment rates",
        "Tertiary": "Tertiary enrollment rates",
    }
    # Rename columns in the DataFrame.
    tb = tb.rename(columns=COLUMNS_RENAME)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year", "sex", "age_group"], verify_integrity=True).sort_index()

    # Drop unnecessary columns
    tb = tb.drop("region", axis=1)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("divorces.xlsx")

    # Load data from snapshot.
    # Load just the sheet with cumulative percentage of marriages ending in divorce by year of marriage and by anniversary, England and Wales, 1963 to 2020.
    tb = snap.read(sheet_name="6a")

    #
    # Process data.
    #
    # Find the row where the first column contains "Country"
    header_row = tb[tb.iloc[:, 0].str.contains("Year of marriage", na=False)].index[0]

    # Set the header row dynamically and drop rows before the header row
    tb.columns = tb.iloc[header_row]
    tb = tb.drop(index=range(header_row + 1)).reset_index(drop=True)
    tb = tb.drop(columns=["Number of marriages"])

    # Melt the DataFrame to create a 'year_of_marriage' column
    tb = tb.melt(id_vars=["Year of marriage"], var_name="anniversary_year", value_name="cumulative_percentage")
    # Keep only numbers in the anniversary_year column
    tb["anniversary_year"] = tb["anniversary_year"].str.extract(r"(\d+)").astype(int)
    # Remove [note 4] and [note 22] from the year columns
    tb["Year of marriage"] = tb["Year of marriage"].str.replace(r"\[note 4\]|\[note 22\]", "", regex=True).astype(int)

    tb = tb.rename(columns={"Year of marriage": "year"})
    tb["country"] = "England and Wales"

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [tb.format(["country", "year", "anniversary_year"])]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()

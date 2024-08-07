"""Load a snapshot and create a meadow dataset."""
import zipfile

from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ethnologue.zip")

    file_names = ["CountryCodes.tab", "LanguageCodes.tab", "LanguageIndex.tab"]
    # Each table has a different index, so we need to specify them here
    indexes = ["CountryID", "LangID", ["LangID", "CountryID", "NameType", "Name"]]
    short_names = ["country_codes", "language_codes", "language_index"]
    # Open the ZIP file and read the .tab file into a pandas DataFrame
    tables = []
    for file_name, index, short_name in zip(file_names, indexes, short_names):
        with zipfile.ZipFile(snap.path, "r") as zip_ref:
            with zip_ref.open(f"Language_Code_Data_20240221/{file_name}") as file:
                tb = pr.read_csv(file, sep="\t", dtype=str)
                if file_name == "LanguageIndex.tab":
                    # There is one erroneous duplicate in the LanguageIndex.tab file - let's remove it here
                    duplicates = tb[tb.duplicated()]
                    assert duplicates.shape[0] == 1
                    tb = tb.drop_duplicates()
                if file_name == "CountryCodes.tab":
                    tb = tb.rename(columns={"Name": "country"}, errors="raise")
                tb = tb.set_index(index)
                tb.metadata.short_name = short_name
                # Want to keep columns as strings, so not using format. Some ID values are 'NA' and 'nan', but aren't missing, categories sets them to missing.
                # tb = tb.format(index, short_name=short_name)
                tables.append(tb)
    # Give the variables the same origins as the snapshot
    for table in tables:
        for col in table.columns:
            table[col].metadata.origins = [snap.metadata.origin]
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tables, check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

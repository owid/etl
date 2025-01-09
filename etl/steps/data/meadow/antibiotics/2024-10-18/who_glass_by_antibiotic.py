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
    snap = paths.load_snapshot("who_glass_by_antibiotic.zip")

    tables = []
    # Load data from snapshot.
    with zipfile.ZipFile(snap.path, "r") as zip_file:
        # Get all csv files in the zip file - for some reason there are duplicated with __MACOSX at the start, we'll drop these
        csv_files = [
            file_name for file_name in zip_file.namelist() if file_name.endswith(".csv") and "__MACOSX" not in file_name
        ]
        for file_name in csv_files:
            tb = snap.read_in_archive(
                filename=file_name,
                skiprows=8,
                encoding="ISO-8859-1",
            )
            # Read in the filters from the csv file which contain important information on the slice of data
            filters = snap.read_in_archive(filename=file_name, nrows=6, header=None, usecols=[0], encoding="ISO-8859-1")
            tb.columns = [
                "country",
                "bcis_per_million",
                "total_bcis",
                "bcis_with_ast_per_million",
                "total_bcis_with_ast",
                "share_bcis_with_ast",
            ]
            # adding additional columns of key information stored in the csv
            tb["year"] = filters.iloc[1, 0].split(":")[-1]
            tb["syndrome"] = filters.iloc[3, 0].split(":")[-1]
            tb["pathogen"] = filters.iloc[4, 0].split(":")[-1]
            tb["antibiotic"] = filters.iloc[5, 0].split(":")[-1]
            assert all(
                tb[["year", "syndrome", "pathogen", "antibiotic"]].notna()
            ), f"missing key information in {file_name}"
            tables.append(tb)

    tb = pr.concat(tables)
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "syndrome", "pathogen", "antibiotic"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("who_glass_by_antibiotic.zip")

    tables = []
    # Load data from snapshot.
    with snap.extracted() as archive:
        # Get all csv files in the zip file - for some reason there are duplicated with __MACOSX at the start, we'll drop these
        csv_files = [f for f in archive.glob("**/*.csv") if "__MACOSX" not in f]
        for file_name in csv_files:
            tb = archive.read(
                file_name,
                skiprows=8,
                encoding="ISO-8859-1",
            )
            # Read in the filters from the csv file which contain important information on the slice of data
            filters = pd.read_csv(archive.path / file_name, nrows=6, header=None, usecols=[0], encoding="ISO-8859-1")
            tb.columns = [
                "country",
                "bcis_per_million",
                "total_bcis",
                "bcis_with_ast_per_million",
                "total_bcis_with_ast",
                "share_bcis_with_ast",
            ]
            # adding additional columns of key information stored in the csv
            year = str(filters.iloc[1, 0]).split(":")[-1].strip()
            syndrome = str(filters.iloc[3, 0]).split(":")[-1].strip()
            pathogen = str(filters.iloc[4, 0]).split(":")[-1].strip()
            antibiotic = str(filters.iloc[5, 0]).split(":")[-1].strip()
            assert all(v not in ("", "nan") for v in [year, syndrome, pathogen, antibiotic]), (
                f"missing key information in {file_name}: year={year!r}, syndrome={syndrome!r}, pathogen={pathogen!r}, antibiotic={antibiotic!r}"
            )
            tb["year"] = year
            tb["syndrome"] = syndrome
            tb["pathogen"] = pathogen
            tb["antibiotic"] = antibiotic
            tables.append(tb)

    tb = pr.concat(tables)

    # remove duplicates
    tb = tb[~(tb.duplicated(subset=["country", "year", "syndrome", "pathogen", "antibiotic"], keep="first"))]

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "syndrome", "pathogen", "antibiotic"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

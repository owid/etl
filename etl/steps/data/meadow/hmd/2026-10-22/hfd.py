"""Load a snapshot and create a meadow dataset.

The snapshot is a zip with one whitespace-separated text file per HFD table (e.g. `tfrRR.txt`, `asfrVHbo.txt`). Each file
starts with a two-line header (table title + blank line), followed by the column names.

Tables come in different dimensions (by year, cohort, age, age reached during the year, or fertility-table age `x`). We
don't hardcode them: each table is indexed by whichever of the dimension columns in `INDEX_COLUMNS` it contains.
"""

import re

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns that identify a row (dimensions), in the order used for the index. Any other column is an indicator.
INDEX_COLUMNS = ["Code", "Year", "Age", "Cohort", "ARDY", "x"]
# Number of tables expected in the snapshot.
NUM_TABLES = 56


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("hfd.zip")

    # Load data from snapshot.
    tbs = []
    with snap.extracted() as archive:
        files = archive.glob("*.txt")
        assert len(files) == NUM_TABLES, f"Expected {NUM_TABLES} tables in the snapshot, found {len(files)}."

        for f in files:
            tb = archive.read(f, force_extension="csv", sep=r"\s+", skiprows=2, na_values=["."])

            # Fertility tables (pft, pftc) have both `lNx` and `LNx` columns, which would clash once lowercased.
            tb = tb.rename(columns={c: f"cap_{c.lower()}" for c in tb.columns if re.fullmatch(r"L\dx", c)})

            # Index by the dimension columns present in the table.
            cols_index = [c for c in INDEX_COLUMNS if c in tb.columns]
            assert cols_index[:1] == ["Code"] and len(cols_index) >= 2, f"Unexpected index columns in {f}: {cols_index}"
            tbs.append(tb.format([c.lower() for c in cols_index], short_name=f.removesuffix(".txt")))

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=tbs,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()

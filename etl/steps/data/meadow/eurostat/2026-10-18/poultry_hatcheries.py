"""Load a snapshot and create a meadow dataset."""

import gzip

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns of the Eurostat file, and their new names.
COLUMNS = {"hatchitm": "item", "animals": "chick_type", "geo\\TIME_PERIOD": "country"}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and read its compressed TSV file.
    snap = paths.load_snapshot("poultry_hatcheries.gz")
    with gzip.open(snap.path, "rt", encoding="utf-8") as f:
        tb = pr.read_csv(
            f, sep=r",|\t", engine="python", metadata=snap.to_table_metadata(), origin=snap.metadata.origin
        )

    #
    # Process data.
    #
    # Column names of years have trailing spaces.
    tb = tb.rename(columns={column: column.strip() for column in tb.columns}).rename(columns=COLUMNS, errors="raise")
    error = "Unexpected frequency or unit."
    assert set(tb["freq"]) == {"A"} and set(tb["unit"]) == {"THS"}, error
    tb = tb.drop(columns=["freq", "unit"])

    # Reshape to long format. Each cell holds a value in thousands (or ":" when missing) followed by optional flags, e.g.
    # "12.3 e" (estimated), "0 n" (not significant), ": @C" (confidential).
    # NOTE: Melting and string operations lose the columns' metadata, which is restored from an original year column.
    year_column = tb[tb.columns[-1]]
    tb = tb.melt(id_vars=list(COLUMNS.values()), var_name="year", value_name="value")
    parts = pd.Series(tb["value"].astype(str).str.strip().values).str.split(pat=" ", n=1, expand=True)
    tb["value"] = pd.to_numeric(parts[0].replace(":", None), errors="raise").values
    tb["value"] = tb["value"].copy_metadata(year_column)
    tb["flag"] = parts[1].fillna("").values
    tb["flag"] = tb["flag"].copy_metadata(year_column)
    tb["year"] = tb["year"].astype(int)

    # Improve table format.
    tb = tb.format(["country", "item", "chick_type", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()

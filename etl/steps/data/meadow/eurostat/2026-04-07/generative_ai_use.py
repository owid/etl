"""Load a snapshot and create a meadow dataset."""

import gzip

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("generative_ai_use.gz")

    # Read compressed TSV file.
    with gzip.open(snap.path, "rt", encoding="utf-8") as f:
        tb = pr.read_csv(
            f, sep=r",|\t", engine="python", metadata=snap.to_table_metadata(), origin=snap.metadata.origin
        )

    #
    # Process data.
    #
    # Rename column: remove "\\TIME_PERIOD" suffix from geo column if present.
    tb = tb.rename(columns={col: col.replace("\\TIME_PERIOD", "") for col in tb.columns})

    # Identify index columns (non-year columns).
    index_columns = [col for col in tb.columns if not col[0].isdigit()]

    # Melt to long format with a "year" column.
    tb = tb.melt(id_vars=index_columns, var_name="year", value_name="value")

    # Drop the "freq" column (always annual).
    assert set(tb["freq"].unique()) == {"A"}, "Unexpected frequency values."
    tb = tb.drop(columns=["freq"])

    # Convert year to integer.
    tb["year"] = tb["year"].str.strip().astype(int)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["ind_type", "indic_is", "unit", "geo", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])

    # Save changes in the new meadow dataset.
    ds_meadow.save()

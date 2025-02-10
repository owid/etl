"""Load a snapshot and create a meadow dataset."""

import gzip

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("producer_prices_in_industry.gz")

    # Read compressed file.
    with gzip.open(snap.path, "rt", encoding="utf-8") as f:
        tb = pr.read_csv(
            f, sep=r",|\t", engine="python", metadata=snap.to_table_metadata(), origin=snap.metadata.origin
        )
    # Identify index columns.
    index_columns = [column for column in tb.columns if not column[0].isdigit()]
    # Melt the table to have a single "time" column.
    tb = tb.melt(id_vars=index_columns, var_name="time", value_name="value")
    # Remove spurious "TIME_PERIOD" from one of the columns.
    tb = tb.rename(columns={column: column.replace("\\TIME_PERIOD", "") for column in tb.columns})

    #
    # Process data.
    #
    # The data has different units, namely:
    # [I21] Index, 2021=100
    # [I15] Index, 2015=100
    # [I10] Index, 2010=100
    # We choose base 2021, which has more recent data.
    tb = tb[tb["unit"] == "I21"].reset_index(drop=True)
    tb = tb.drop(columns=["unit"], errors="raise")
    # Clafity this choice in the metadata (to avoid confusion in the garden step).
    tb["value"].description_short = "Index value of the producer prices in industry, expressed relative to 2021."

    # The "freq" field is unnecessary (since all data is monthly).
    assert set(tb["freq"]) == {"M"}, "Unexpected option in 'freq' column."
    tb = tb.drop(columns=["freq"], errors="raise")

    # Drop unnecessary columns.
    assert set(tb["indic_bt"]) == {"PRC_PRR"}, "Unexpected option in 'indic_bt' column."
    assert set(tb["s_adj"]) == {"NSA"}, "Unexpected option in 's_adj' column."
    tb = tb.drop(columns=["indic_bt", "s_adj"], errors="raise")

    # Remove rows without data.
    tb = tb[~tb["value"].str.contains(":")].reset_index(drop=True)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["nace_r2", "geo", "time"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

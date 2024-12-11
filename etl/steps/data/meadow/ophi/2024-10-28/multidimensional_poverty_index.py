"""Load a snapshot and create a meadow dataset."""

from typing import List

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define index columns for hot and cot tables.
INDEX_COLS = [
    "country",
    "year",
    "loa",
    "measure",
    "indicator",
    "region_lab",
    "area_lab",
    "agec2_lab",
    "agec4_lab",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_cme = paths.load_snapshot("multidimensional_poverty_index_cme.csv")
    snap_hot = paths.load_snapshot("multidimensional_poverty_index_hot.csv")

    # Load data from snapshot.
    tb_cme = snap_cme.read()
    tb_hot = snap_hot.read()

    #
    # Process data.
    #
    # Format columns and index.
    tb_cme = format_columns_and_index(tb=tb_cme, short_name="cme", index_columns=INDEX_COLS)
    tb_hot = format_columns_and_index(tb=tb_hot, short_name="hot", index_columns=INDEX_COLS)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb_cme, tb_hot], check_variables_metadata=True, default_metadata=snap_cme.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def format_columns_and_index(tb: Table, short_name: str, index_columns: List[str]) -> Table:
    """
    Rename columns, format year and select the categories I need.
    """
    # Rename columns.
    tb = tb.rename(columns={"cty_lab": "country"})

    # Make year string
    tb["year"] = tb["year"].astype("string")

    # In the measure column, select all the categories, except for pctb
    tb = tb[tb["measure"] != "pctb"].reset_index(drop=True)

    tb = tb[~tb["loa"].isin(["hship", "agec2", "agec4", "region"])].reset_index(drop=True)

    # NOTE: On years
    # As the year data is encoded in a string variable between two years, we need to map the data to a single (integer) year.
    # For now, arbitrarily, I take the first year in these cases and convert to integer.

    tb["year"] = tb["year"].str[:4].astype(int)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(
        index_columns,
        short_name=short_name,
    )

    return tb

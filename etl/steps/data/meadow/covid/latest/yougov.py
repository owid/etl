"""Load a snapshot and create a meadow dataset."""

from typing import List

import pandas as pd
from owid.catalog import Table
from owid.catalog.processing import concat

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

ENCODINGS = {
    "united-kingdom.zip": "cp1252",
}
DEPENDENCIES_IGNORE = ["yougov_extra_mapping.csv"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve aux table
    tb_mapping = read_table_from_snap("yougov_extra_mapping.csv")
    tb_mapping = process_mapping_table(tb_mapping)

    # Retrieve country snapshots.
    dependencies = [d.split("/")[-1] for d in paths.dependencies]
    tables = []
    for d in dependencies:
        if d in DEPENDENCIES_IGNORE:
            continue
        paths.log.info(d)
        # Read table
        t = read_table_from_snap(d)
        # Select columns (decrease requirements)
        t = select_subset(t, tb_mapping)
        tables.append(t)

    #
    # Process data.
    #
    # Combine all tables
    tb = combine_tables(tables)
    # Parse date
    tb = parse_date(tb)
    # Drop NaNs
    tb = tb.dropna(subset=["date"])
    # Format
    tb["identifier"] = tb.index
    tb = tb.format(["identifier"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def read_table_from_snap(short_name: str) -> Table:
    """Read snapshot as table."""
    snap = paths.load_snapshot(short_name)
    kwargs = {
        "low_memory": False,
        "na_values": [
            "",
            "Not sure",
            " ",
            "Prefer not to say",
            "Don't know",
            98,
            "Don't Know",
            "Not applicable - I have already contracted Coronavirus (COVID-19)",
            "Not applicable - I have already contracted Coronavirus",
        ],
    }
    # Set encoding if needed.
    encoding = ENCODINGS.get(short_name.replace("yougov_", ""))
    if encoding is not None:
        kwargs["encoding"] = encoding

    # Read
    t = snap.read_csv(**kwargs)

    return t


def process_mapping_table(tb: Table) -> Table:
    """Sanity check & process mapping table."""
    # Standardise `label` column
    tb["label"] = tb["label"].str.lower()

    # Sanity checks on tb_mapping
    assert tb["keep"].isin([True, False]).all(), 'All values in "keep" column of `tb_mapping` must be True or False.'
    assert tb["code_name"].duplicated().sum() == 0, "All rows in the `code_name` field of mapping.csv must be unique."

    # Select columns in tb_mapping that we will keep
    tb = tb.loc[tb["keep"] & ~tb["derived"]]
    return tb


def short_name_to_country_name(short_name: str) -> str:
    """Extract country name from short_name."""
    return short_name.replace("yougov_", "").replace("-", " ").replace("_", " ").title()


def combine_tables(tables: List[Table]) -> Table:
    """Combine tables.

    Assigns country name to each table before concatenating them.
    """
    for t in tables:
        assert isinstance(t.m.short_name, str), "short_name must be a string"
        t["country"] = short_name_to_country_name(short_name=t.m.short_name)
    tb = concat(tables, short_name="yougov", ignore_index=True)
    return tb


def parse_date(tb: Table) -> Table:
    """Format date column."""
    tb.loc[:, "date"] = pd.to_datetime(tb["endtime"], format="%d/%m/%Y %H:%M")
    return tb


def select_subset(tb: Table, tb_mapping: Table):
    """Keep only the survey questions with keep=True in tb_mapping and renames columns."""
    # Select relevant columns
    date_col = ["endtime"]
    cols = list(set(c for c in tb_mapping["label"].tolist() if c in tb.columns))

    tb = tb.loc[:, date_col + cols]

    # There are some columns which are mapped to multiple columns
    x = tb_mapping.groupby("label")["code_name"].nunique()
    cols_multiple = set(x[x > 1].index)

    # Rename columns
    rename = tb_mapping.set_index("label")["code_name"].to_dict()
    rename = {k: v for k, v in rename.items() if k not in cols_multiple}
    tb = tb.rename(columns=rename)

    # Copy columns that are mapped to multiple columns
    for col in cols_multiple:
        if col in tb.columns:
            columns_new = tb_mapping.loc[tb_mapping["label"] == col, "code_name"].tolist()
            for col_new in columns_new:
                tb[col_new] = tb[col]
            tb = tb.drop(columns=col)
    return tb

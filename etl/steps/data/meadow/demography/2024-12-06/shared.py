"""Load a snapshot and create a meadow dataset.

This snapshot step is a bit more complex than usual. This is because the snapshot is a ZIP file that contains numerous RDS files. These RDS files can be merged and concatenated, so that we build a single table with all the data.


The output table has index columns: country, year, scenario, sex, age, education.

When values are aggregates, dimensions are set to "total".
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import owid.catalog.processing as pr
import pyreadr
from owid.catalog import Table
from owid.catalog.tables import _add_table_and_variables_metadata_to_table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Renaming of relevant columns
COLUMNS_RENAME = {
    "name": "country",
    "period": "year",
}

# Harmonization of the dimension values
REPLACE_AGE = {
    "all": "total",
}
REPLACE_SEX = {
    "both": "total",
}


def make_scenario_tables(tbs_scenario, tables_combine_edu, tables_concat, tables_drop, tables_composition):
    """Create main table from all scenarios.

    Index: country, year, scenario, age, sex, education.
    """
    # Obtain tables
    tbs_base = []
    for scenario, tbs in tbs_scenario.items():
        paths.log.info(f"> scenario {scenario}")
        tb = make_tables_from_scenario(
            tbs=tbs,
            scenario_num=scenario,
            tables_combine_edu=tables_combine_edu,
            tables_concat=tables_concat,
            tables_drop=tables_drop,
            tables_composition=tables_composition,
        )
        tbs_base.append(tb)

    # Re-shape table structure
    tbs_ = {group: [] for group in tables_composition}
    for tbs in tbs_base:
        for key, tb in tbs.items():
            tbs_[key].append(tb)

    return tbs_


def concatenate_tables(tbs_scenario):
    tables = []
    for tname, tbs in tbs_scenario.items():
        paths.log.info(f"Concatenating table {tname}")
        # Check columns match for all tables in group
        cols_index = None
        for tb in tbs:
            cols_index_ = _get_index_columns(tb)
            if cols_index is None:
                cols_index = list(cols_index_)
            else:
                assert set(cols_index) == set(cols_index_), "Unexpected index columns!"
        # Merge
        tb = pr.concat(tbs, short_name=tname)

        # Format
        paths.log.info(f"Formatting table {tname}")
        assert isinstance(cols_index, list)
        tb = tb.format(cols_index + ["scenario"])

        # Add to main list
        tables.append(tb)
    return tables


def make_tables_from_scenario(tbs, scenario_num, tables_combine_edu, tables_concat, tables_drop, tables_composition):
    """Integrate all tables from scenario into single tables.

    We generate multiple single tables since different tables may use different dimensions. This helps in optimizing computation.
    """
    # Sanity check country - country_code match
    tables = []
    for tb in tbs:
        # Check columns are in tables
        assert ("country" in tb.columns) and ("country_code" in tb.columns), "Missing country or country_code!"
        # Check there is a one-to-one correspondence
        assert (
            tb.groupby("country")["country_code"].nunique().max() == 1
        ), "Multiple country codes for a single country!"
        # Drop country_code
        tb = tb.drop(columns="country_code")
        tables.append(tb)

    # Create dictionary to ease navigation
    tables = {t.m.short_name: t for t in tables}
    # Dictionary with table combinations made
    tables = reduce_tables(tables, tables_combine_edu, tables_concat, tables_drop)

    # Composition of tables
    tables = consolidate_table_all(tables, scenario_num, tables_composition)

    return tables


def reduce_tables(tables, tables_combine_edu, tables_concat, tables_drop):
    """Reduces the original number based on similar indicators.

    Given a key-value dictionary with all the tables, this function simplifies it structure:

    - It combines different tables into a single one. That is possible when, e.g. a table contains the same indicator but broken down by an additional dimension.
    - Some tables don't have new data. These can be discarded.
    """
    paths.log.info("Reducing tables...")
    # Start by defining the output dictionary `tables_reduced`, with those tables that are not combined and thus should be kept, at least for now, as they are
    tables_combined = [cc for c in tables_combine_edu for cc in c] + [cc for c in tables_concat for cc in c]
    if ("net" in tables) and ("netedu" in tables):
        tables_combined += ["net", "netedu"]
    tables_not_combined = [name for name in tables.keys() if name not in tables_combined]
    tables_reduced = {name: harmonize_tb(tables[name]) for name in tables_not_combined}

    # Iterate over the tables from `tables_combine_edu` to consolidate the pairs into single representation.
    # Index 1 contains data broken down by 'education'. Index 0 contains data with 'education' = 'total'.
    for tb_comb in tables_combine_edu:
        # Load tables
        tb1 = tables[tb_comb[0]]
        tb2 = tables[tb_comb[1]]
        # Drop in special case
        if tb_comb[0] == "bpop":
            tb1 = tb1.loc[tb1["age"] != "All"]
        # Prepare tables for merge
        tb1 = tb1.assign(education="total")
        tb2 = tb2.rename(columns={tb_comb[1]: tb_comb[0]})
        # Check: columns are identical except 'education'
        assert set(tb1.columns) == set(tb2.columns), "Unexpected columns!"
        # Harmonize
        tb1 = harmonize_tb(tb1)
        tb2 = harmonize_tb(tb2)
        # Concatenate
        tb = pr.concat([tb1, tb2], ignore_index=True).drop_duplicates()
        # Add to dictionary
        tables_reduced[tb_comb[0]] = tb
    for tb_comb in tables_concat:
        # Load tables
        tb1 = tables[tb_comb[0]]
        tb2 = tables[tb_comb[1]]
        # Prepare tables for merge
        tb2 = tb2.rename(columns={tb_comb[1]: tb_comb[0]})
        # Check: columns are identical except 'education'
        assert set(tb1.columns) == set(tb2.columns), "Unexpected columns!"
        # Harmonize
        tb1 = harmonize_tb(tb1)
        tb2 = harmonize_tb(tb2)
        # Concatenate
        tb = pr.concat([tb1, tb2], ignore_index=True).drop_duplicates()
        # Add to dictionary
        tables_reduced[tb_comb[0]] = tb

    # Special case: net and netedu
    if ("net" in tables) and ("netedu" in tables):
        # net has age=all and sex=all, so we can drop these columns
        # NOTE: net is equivalent to sum(netedu) + epsilon (probably unknown education?)
        tb1 = tables["net"]
        tb2 = tables["netedu"]
        assert set(tb1["age"].unique()) == {"All"}
        assert set(tb1["sex"].unique()) == {"Both"}
        tb1 = tb1.drop(columns=["age", "sex"])
        tb1["education"] = "total"
        # Rename
        tb2 = tb2.rename(columns={"netedu": "net"})
        # Check: columns are identical except 'education'
        assert set(tb1.columns) == set(tb2.columns), "Unexpected columns!"
        # Harmonize
        tb1 = harmonize_tb(tb1)
        tb2 = harmonize_tb(tb2)
        # Concatenate
        tb = pr.concat([tb1, tb2], ignore_index=True).drop_duplicates()
        tables_reduced["net"] = tb

    # Remove tables that are not needed
    tables_reduced = {tname: tb for tname, tb in tables_reduced.items() if tname not in tables_drop}

    # Special: rename 'bpop' -> 'pop'
    tables_reduced["pop"] = tables_reduced["bpop"].rename(columns={"bpop": "pop"})
    del tables_reduced["bpop"]

    # Sort dictionary by keys
    tables_reduced = dict(sorted(tables_reduced.items()))
    return tables_reduced


def consolidate_table_all(tables, scenario_num, tables_composition):
    """Consolidate tables into new groups.

    Each table group is differentiated from the rest based on the index its tables use.
    Grouping by index helps make the merge of all tables way faster.
    The idea is that in garden, we process & export each table separately.
    This function also harmonizes dimension names.
    """
    # Group tables in new groups
    tables_new = {}
    for tname_new, tnames in tables_composition.items():
        paths.log.info(f"Building {tname_new}...")
        # Get list with table objects, with harmonized dimensions
        tbs_ = []
        # Index columns
        cols_index = None
        for tname in tnames:
            # Get table
            tb_ = tables[tname]
            # Harmonize table
            tb_ = harmonize_tb(tb_)
            # Check dimensions
            cols_index_ = _get_index_columns(tb_)
            if cols_index is None:
                cols_index = list(cols_index_)
            else:
                assert set(cols_index) == set(cols_index_), "Unexpected index columns!"
            tbs_.append(tb_)
        # Merge all tables in list
        tb = merge_tables_opt(tbs_, on=cols_index, how="outer")
        # Add scenario information
        tb["scenario"] = scenario_num
        tb["scenario"] = tb["scenario"].astype("string")
        # Add consolidated table in main dictionary
        tables_new[tname_new] = tb
    return tables_new


def _get_index_columns(tb):
    cols_index_all = ["country", "year", "sex", "age", "education"]
    cols_index = list(tb.columns.intersection(cols_index_all))
    return cols_index


def harmonize_tb(tb):
    """Harmonizes tables.

    - Dimensions are named differently in different tables. This function ensures that they are consistent.
    - Makes sure DTypes are set correctly.
    """
    if "age" in tb.columns:
        tb["age"] = (
            tb["age"]
            .str.lower()
            .replace(REPLACE_AGE)
            .str.replace("––", "-", regex=False)
            .str.replace("--", "-", regex=False)
        ).astype("string")
    if "sex" in tb.columns:
        tb["sex"] = tb["sex"].str.lower().replace(REPLACE_SEX).astype("string")
    if "education" in tb.columns:
        tb["education"] = tb["education"].str.lower().str.replace(" ", "_").astype("string")

    # Set dtype
    tb = tb.astype(
        {
            "country": "string",
            "year": "string",
        }
    )
    return tb


def merge_tables_opt(tables, **kwargs):
    """Optimized & parallelized version of merge_tables."""

    def _merge_pair(tables):
        """Merge two tables."""
        left, right = tables
        return left.merge(right, **kwargs)

    # Divide tables into pairs to merge in parallel
    with ThreadPoolExecutor() as executor:
        while len(tables) > 1:
            # Pair tables and merge them in parallel
            future_to_merge = {
                executor.submit(_merge_pair, (tables[i], tables[i + 1])): i for i in range(0, len(tables) - 1, 2)
            }

            # Collect merged tables
            merged_tables = []
            for future in as_completed(future_to_merge):
                merged_tables.append(future.result())

            # If odd number of tables, append the last unpaired table
            if len(tables) % 2 == 1:
                merged_tables.append(tables[-1])

            # Update tables list with merged results
            tables = merged_tables

    # Final merged table
    merged_tb = tables[0]
    return merged_tb


def read_data_from_snap(snap, scenarios_expected):
    """Read snapshot.

    Snapshot is a ZIP file that contains numerous RDS files.
    """
    tbs_scenario = {}
    with snap.extract_to_tempdir() as tmp:
        files = os.listdir(tmp)
        for i, f in enumerate(files):
            if i % 10 == 0:
                paths.log.info(f"Processing file {i}/{len(files)}: {f}")
            # Add relevant columns
            scenario = f.split("_")[0]
            assert scenario in scenarios_expected, f"Unexpected scenario: {scenario}"
            if scenario in {"22", "23"}:
                continue
            # Read RDS file
            path = tmp / Path(f)
            if f.endswith(".rds"):
                data = pyreadr.read_r(path)
                assert set(data.keys()) == {None}, "Unexpected keys in RDS file!"
                df = data[None]
                # Map to table
                tb = Table(
                    df,
                    short_name=f.split("_")[1].replace(".rds", ""),
                )
                tb = _add_table_and_variables_metadata_to_table(
                    table=tb, metadata=snap.to_table_metadata(), origin=snap.metadata.origin
                )
                tb.metadata.short_name = f.split("_")[1].replace(".rds", "")
            elif f.endswith(".csv.gz"):
                tb = pr.read_csv(
                    path,
                    metadata=snap.to_table_metadata(),
                    origin=snap.metadata.origin,
                )
                tb.metadata.short_name = f.split("_")[1].replace(".csv.gz", "")
            else:
                raise ValueError(f"Unexpected file format: {f}!")
            # Rename columns
            tb = tb.rename(columns=COLUMNS_RENAME)
            # Add to main list
            if scenario in tbs_scenario.keys():
                tbs_scenario[scenario].append(tb)
            else:
                tbs_scenario[scenario] = [tb]
    return tbs_scenario


def inspect_tbs_in_scenario(tbs):
    """For exploration purposes only."""
    cols_index = ["country", "country_code", "scenario", "year", "age", "sex", "education"]
    for tname, tb in tbs.items():
        cols = set(tb.columns)
        cols_index_ = [col for col in cols_index if col in cols]
        cols_indicators_ = [col for col in cols if col not in cols_index_]
        # cols_ = list(sorted(cols_index_)) + list(sorted(cols_indicators_))
        print(f"> {', '.join(sorted(cols_indicators_))} ({tname}): {', '.join(cols_index_)}")
        # print(f"{', '.join(cols_index_)}")

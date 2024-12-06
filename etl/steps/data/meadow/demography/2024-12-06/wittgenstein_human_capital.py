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

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SCENARIOS_EXPECTED = {
    "1",
    "2",
    "22",
    "23",
    "3",
    "4",
    "5",
}
COLUMNS_RENAME = {
    "name": "country",
    "period": "year",
}

REPLACE_AGE = {
    "all": "total",
}
REPLACE_SEX = {
    "both": "total",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("wittgenstein_human_capital.zip")

    # Load data from snapshot.
    tbs_scenario = read_data_from_snap(snap)

    # Consolidate
    tb = make_table(tbs_scenario)

    # Repeated columns: pop, ggapmys
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "scenario", "sex", "age", "education"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


# with snap.extract_to_tempdir() as tmp:
#     path = tmp / Path("1_assr.rds")
#     parsed = rdata.parser.parse_file(path, extension="rds")  # type: ignore


# with snap.extract_to_tempdir() as tmp:
#     path = tmp / Path("1_assr.rds")
#     result = pyreadr.read_r(path)


def make_table(tbs):
    """Create main table from all scenarios.

    Index: country, year, scenario, age, sex, education.
    """
    tbs_ = []
    for scenario, tbs_scenario in tbs.items():
        print(f"scenario {scenario}")
        tb = make_table_from_scenario(tbs_scenario)
        tb["scenario"] = scenario
        tb["scenario"] = tb["scenario"].astype("string")
        tbs_.append(tb)
    tb = pr.concat(tbs_, ignore_index=True)
    return tb


def make_table_from_scenario(tbs):
    """Integrate all tables from scenario into a single table"""
    # Separate population from the rest
    tbs_all, tbs_pop = separate_population_from_rest(tbs)
    # Consolidate all population metrics into one single table with dimensions
    tb_pop = consolidate_population(tbs_pop)
    # Consolidate all other metrics into one single table with dimensions
    tb_all = consolidate_table_all(tbs_all)
    # Merge both tables
    tb = tb_all.merge(tb_pop, on=["country", "year", "sex", "age", "education"], how="outer")
    return tb


def separate_population_from_rest(tbs):
    """Separate the tables into two main groups: population and the rest.

    This is because there are numerous population tables and these have to be consolidated on their own before attempting to merge them with the rest of the tables.
    """
    tbs_all = []
    tbs_pop = []
    for tb in tbs:
        if tb.m.short_name.startswith("pop"):
            tbs_pop.append(tb)
        else:
            tbs_all.append(tb)
    return tbs_all, tbs_pop


def consolidate_population(tbs):
    # Get main table (contains data broken down by age, sex)
    tb_main = [tb for tb in tbs if tb.m.short_name == "pop"][0]
    tb_main["education"] = "total"

    # Get data by education
    tb_edu = [tb for tb in tbs if tb.m.short_name == "pop-age-sex-edattain"][0]
    tb_edu_all_sex = [tb for tb in tbs if tb.m.short_name == "pop-age-edattain"][0]
    tb_edu_all_sex["sex"] = "total"
    tb_edu_all_ages = [tb for tb in tbs if tb.m.short_name == "pop-sex-edattain"][0]
    tb_edu_all_ages["age"] = "total"
    # tb_edu = pr.concat([tb_edu, tb_edu_all_sex, tb_edu_all_ages], ignore_index=True)
    # _ = tb_edu.format(["country", "year", "age", "sex", "education"])

    # Concatenate all tables
    tb = pr.concat([tb_edu, tb_edu_all_sex, tb_edu_all_ages], ignore_index=True)

    # Final cleaning
    tb = harmonize_tb(tb)

    return tb


def consolidate_table_all(tbs):
    tbs_ = []
    for tb in tbs:
        if "sex" not in tb:
            tb["sex"] = "total"
        if "age" not in tb:
            tb["age"] = "total"
        if "education" not in tb:
            tb["education"] = "total"
        # Final cleaning
        tb = harmonize_tb(tb)
        # Append to list
        tbs_.append(tb)

    tb = merge_tables_opt(tbs_, on=["country", "year", "sex", "age", "education"], how="outer")
    return tb


def harmonize_tb(tb):
    """Harmonizes tables.

    - Dimensions are named differently in different tables. This function ensures that they are consistent.
    - Makes sure DTypes are set correctly.
    """
    tb["age"] = (
        tb["age"]
        .str.lower()
        .replace(REPLACE_AGE)
        .str.replace("––", "-", regex=False)
        .str.replace("--", "-", regex=False)
    )
    tb["sex"] = tb["sex"].str.lower().replace(REPLACE_SEX)
    tb["education"] = tb["education"].str.lower().str.replace(" ", "_")

    # Drop unused column
    tb = tb.drop(columns="country_code")

    # Set dtype
    tb = tb.astype(
        {
            "country": "string",
            "year": "string",
            "age": "string",
            "sex": "string",
            "education": "string",
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


def read_data_from_snap(snap):
    """Read snapshot.

    Snapshot is a ZIP file that contains numerous RDS files.
    """
    tbs_scenario = {}
    with snap.extract_to_tempdir() as tmp:
        files = os.listdir(tmp)
        for i, f in enumerate(files):
            if i % 10 == 0:
                print(f"Processing file {i}/{len(files)}: {f}")
            # Read RDS file
            path = tmp / Path(f)
            data = pyreadr.read_r(path)
            assert set(data.keys()) == {None}, "Unexpected keys in RDS file!"
            df = data[None]
            # Add relevant columns
            scenario = f.split("_")[0]
            assert scenario in SCENARIOS_EXPECTED, f"Unexpected scenario: {scenario}"
            # Rename columns
            df = df.rename(columns=COLUMNS_RENAME)
            # Map to table
            tb = Table(df, short_name=f.split("_")[1].replace(".rds", ""))
            # Add to main list
            if scenario in tbs_scenario.keys():
                tbs_scenario[scenario].append(tb)
            else:
                tbs_scenario[scenario] = [tb]
    return tbs_scenario


def inspect_tbs_in_scenario(tbs):
    """For exploration purposes only."""
    cols_index = ["country", "country_code", "scenario", "year", "age", "sex", "education"]
    for tb in tbs:
        cols = set(tb.columns)
        cols_index_ = [col for col in cols_index if col in cols]
        cols_indicators_ = [col for col in cols if col not in cols_index_]
        # cols_ = list(sorted(cols_index_)) + list(sorted(cols_indicators_))
        # print(cols_index_)
        print("\t" + f"{list(sorted(cols_indicators_))}")

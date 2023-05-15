"""Load a snapshot and create a meadow dataset."""

from typing import Dict

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("global_wellbeing: start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("global_wellbeing.xlsx")

    # Load data from snapshot.
    dfs = pd.read_excel(snap.path, sheet_name=None, header=[0, 1, 2])

    # Check sheets are as expected
    log.info("global_wellbeing: sanity checking input")
    sanity_checks(dfs)

    # Load individual dataframes
    log.info("global_wellbeing: loading dataframes")
    df_countries = dfs["Country Level"]
    df_world = dfs["Global"]

    # Concatenate
    log.info("global_wellbeing: combine dataframes")
    df = combine_dfs(df_countries, df_world)

    # Drop unneceassary column level
    log.info("global_wellbeing: drop level=2 column index")
    df = df.droplevel(level=2, axis=1)

    # Unpivot dataframe.
    # Table object does not accept MultiLevel columns, therefore we do this reshaping here in Meadow
    log.info("global_wellbeing: unpivot")
    col_id = df.columns[:3]
    df = df.melt(id_vars=list(col_id), var_name=["question", "answer"], value_name="share")

    # Create index columns
    log.info("global_wellbeing: build index columns")
    df["country"] = df[df.columns[0]].fillna(method="ffill")
    df["dimension"] = df[df.columns[1]].fillna(method="ffill")
    df["dimension"] = "(" + df["dimension"] + "?) " + df[df.columns[2]]

    # Set index and get only relevant columns
    log.info("global_wellbeing: set index")
    df = df.set_index(["country", "dimension", "question", "answer"], verify_integrity=True)[["share"]]

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("global_wellbeing.end")


def sanity_checks(dfs: Dict[str, pd.DataFrame]):
    sheets_expected = {"Country Level", "Global"}
    sheets = set(dfs.keys())
    sheets_missing = sheets_expected.difference(sheets)
    assert not sheets_missing, f"There are some missing sheets! {sheets_missing}"


def combine_dfs(df_countries: pd.DataFrame, df_world: pd.DataFrame) -> pd.DataFrame:
    df_world.columns = df_countries.columns[1:]
    df_world[df_countries.columns[0]] = "World"
    df = pd.concat([df_countries, df_world], ignore_index=True)
    return df

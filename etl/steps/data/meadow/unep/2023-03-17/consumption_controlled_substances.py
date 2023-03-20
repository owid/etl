"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table
from owid.datautils.dataframes import multi_merge
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
CHEMICAL_NAMES = {
    "bromochloromethane",
    "carbon_tetrachloride",
    "chlorofluorocarbons",
    "halons",
    "hydrobromofluorocarbons",
    "hydrochlorofluorocarbons",
    "hydrofluorocarbons",
    "methyl_bromide",
    "methyl_chloroform",
    "other_fully_halogenated",
}


def run(dest_dir: str) -> None:
    log.info("consumption_controlled_substances.start")

    #
    # Load inputs.
    #
    dfs = []
    for name in CHEMICAL_NAMES:
        log.info(f"consumption_controlled_substances: loading snapshot `{name}`.")
        # Retrieve snapshot.
        snap: Snapshot = paths.load_dependency(f"consumption_controlled_substances.{name}.xlsx")
        # Load data from snapshot.
        df = format_frame(pd.read_excel(snap.path, skiprows=1), name=name)
        # Append to list of dataframes
        dfs.append(df)
    log.info("consumption_controlled_substances: merging dataframes.")
    df = multi_merge(dfs, on=["Country", "year"], how="outer")
    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)  # type: ignore

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("consumption_controlled_substances.end")


def format_frame(df: pd.DataFrame, name: str) -> pd.DataFrame:
    # Ensure no unexpected columns appear. We expect "Country", "Baseline", and year columns.
    YEAR_MIN = 1986
    YEAR_MAX = 2022
    COLUMN_NAMES_ACCEPTED = {"Country", "Baseline"} | set(range(YEAR_MIN, YEAR_MAX + 1))
    assert not (columns_new := set(df.columns).difference(COLUMN_NAMES_ACCEPTED)), f"Unexpected columns {columns_new}"
    # Ensure country column is there
    assert "Country" in df.columns, "Missing country column!"
    # Remove 'Baseline' column if exists
    if "Baseline" in df.columns:
        df = df.drop(columns=["Baseline"])
    # Format df (unpivot)
    df = df.melt(id_vars="Country", var_name="year", value_name=name).astype({"year": int})
    return df

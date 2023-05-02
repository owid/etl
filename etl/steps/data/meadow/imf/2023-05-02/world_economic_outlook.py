"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# This is a preliminary bulk importer for the IMF's WEO dataset.
# As of October 2021, it only generates a grapher-compatible dataset with 1 variable (GDP growth).
# But this first version could be extended to a traditional bulk import of the entire dataset later.
VARIABLE_LIST = ["Gross domestic product, constant prices - Percent change"]


def read(path) -> pd.DataFrame:
    df = (
        pd.read_csv(path, delimiter="\t", encoding="utf-16-le")
        .drop(
            columns=[
                "WEO Country Code",
                "WEO Subject Code",
                "Estimates Start After",
                "ISO",
                "Country/Series-specific Notes",
                "Subject Notes",
                "Scale",
            ]
        )
        .dropna(subset=["Country"])
    )
    df = df.loc[:, ~df.columns.str.contains("Unnamed")]
    return df


def make_variable_names(df: pd.DataFrame) -> pd.DataFrame:
    df["variable"] = df["Subject Descriptor"] + " - " + df["Units"]
    df = df.drop(columns=["Subject Descriptor", "Units"])
    return df


def pick_variables(df: pd.DataFrame) -> pd.DataFrame:
    return df[df.variable.isin(VARIABLE_LIST)]


def reshape_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.melt(id_vars=["Country", "variable"], var_name="year")

    # Coerce values to numeric, and drop NAs
    df = df.assign(value=pd.to_numeric(df.value, errors="coerce")).dropna(subset=["value"])

    df = df.pivot(index=["Country", "year"], columns="variable", values="value").reset_index()
    return df


def run(dest_dir: str) -> None:
    log.info("world_economic_outlook.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("world_economic_outlook.xls")

    # Load data from snapshot, then process.
    df = read(snap.path).pipe(make_variable_names).pipe(pick_variables).pipe(reshape_and_clean)

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("world_economic_outlook.end")

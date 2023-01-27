"""
Load the snapshot for 'Luxembourg Income Study - Absolute poverty' and creates a meadow dataset.
Country names are converted from iso-2 codes in this step and years are reformated from "YY" to "YYYY"
"""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder
from etl.paths import REFERENCE_DATASET
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("lis_abs_poverty.start")

    #
    # Load inputs.
    # Load reference file with country names in OWID standard
    ds_reference = Dataset(REFERENCE_DATASET)
    df_countries_regions = pd.DataFrame(ds_reference["countries_regions"]).reset_index()

    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("lis_abs_poverty.csv")

    # Load data from snapshot.
    df = pd.read_csv(snap.path)

    # Define column types
    id_cols = ["country", "year", "dataset", "variable", "eq", "povline"]
    df[[col for col in df.columns if col not in id_cols]] = df[[col for col in df.columns if col not in id_cols]].apply(
        pd.to_numeric, errors="coerce"
    )

    # Extract country and year from dataset
    df["country"] = df["dataset"].str[:2].str.upper()
    df["year"] = df["dataset"].str[2:4].astype(int)

    # Replace "UK" with "GB" (official ISO-2 name for the United Kingdom)
    df["country"] = np.where(df["country"] == "UK", "GB", df["country"])

    # Create year variable in the format YYYY instead of YY
    df["year"] = np.where(df["year"] > 50, df["year"] + 1900, df["year"] + 2000)

    # Merge dataset and country dictionary to get the name of the country (and rename it as "country")
    df = pd.merge(
        df, df_countries_regions[["name", "iso_alpha2"]], left_on="country", right_on="iso_alpha2", how="left"
    )
    df = df.drop(columns=["country", "iso_alpha2"])
    df = df.rename(columns={"name": "country"})

    # Move country and year to the beginning
    cols_to_move = ["country", "year"]
    df = df[cols_to_move + [col for col in df.columns if col not in cols_to_move]]

    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))

    # Ensure the version of the new dataset corresponds to the version of current step.
    ds_meadow.metadata.version = paths.version

    # Add the new table to the meadow dataset.
    ds_meadow.add(tb)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("lis_abs_poverty.end")

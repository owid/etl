"""Load a snapshot and create a meadow dataset."""

from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("longitudinal_wvs.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("longitudinal_wvs.csv"))

    # Load data from snapshot.
    df = pd.read_csv(snap.path)
    #
    # Process data.
    #
    terrorism_columns = ["COUNTRY_ALPHA", "S020", "H006_04", "G057", "F114E"]
    df = df[terrorism_columns]
    # Replace keys where question was not asked with nan
    cols_to_update = ["H006_04", "G057", "F114E"]
    valid_resp = list(range(11)) + [-1, -2]
    df[cols_to_update] = df[cols_to_update].applymap(lambda x: x if x in valid_resp else np.nan)
    # Rename columns to more informative names
    dictionary_keys = {
        "COUNTRY_ALPHA": "code",
        "S020": "year",
        "H006_04": "Worries: a terrorist attack",
        "G057": "Effects of immigrants on the development of [your country]: Increase the risks of terrorism",
        "F114E": "Justifiable: Terrorism as a political, ideological or religious mean",
    }
    df.rename(columns=dictionary_keys, inplace=True)
    # Load the countries and ISO 3166-1 alpha-3 codes dataset to get the country names based on the  ISO 3166-1 alpha-3 codes in the datagrame
    countries_regions = cast(Dataset, paths.load_dependency("regions"))["regions"]
    iso_match = countries_regions.reset_index()[["code", "name"]]
    match_code_df = pd.merge(iso_match, df, on="code", how="right")
    # Drop the country column code and rename the country 'name' column to 'country' column
    match_code_df.drop("code", axis=1, inplace=True)
    match_code_df.rename(columns={"name": "country"}, inplace=True)

    # Create a new table and ensure all columns are snake-case.
    tb = Table(match_code_df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("longitudinal_wvs.end")

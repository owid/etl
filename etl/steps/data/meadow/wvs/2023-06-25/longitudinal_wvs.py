"""Load a snapshot and create a meadow dataset."""

from typing import cast

import numpy as np
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
    log.info("longitudinal_wvs.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("longitudinal_wvs.csv"))

    # Load data from snapshot.
    df = pd.read_csv(snap.path)
    terrorism_columns = ["COUNTRY_ALPHA", "S020", "H006_04", "G057", "E206", "F114E"]
    df = df[terrorism_columns]
    # replace keys where question was not asked with nan (1,2,3,4 are the only valid answers)
    cols_to_update = ["H006_04", "G057", "E206", "F114E"]
    df[cols_to_update] = df[cols_to_update].applymap(lambda x: x if x in [1, 2, 3, 4] else np.nan)

    dictionary_keys = {
        "COUNTRY_ALPHA": "country",
        "S020": "year",
        "H006_04": "Worries: a terrorist attack",
        "G057": "Effects of immigrants on the development of [your country]: Increase the risks of terrorism",
        "E206": "Free and fair elections will reduce terrorism",
        "F114E": "Justifiable: Terrorism as a political, ideological or religious mean",
    }
    df.rename(columns=dictionary_keys, inplace=True)

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

    log.info("longitudinal_wvs.end")

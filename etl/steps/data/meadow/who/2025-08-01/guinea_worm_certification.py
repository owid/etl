"""Load a snapshot and create a meadow dataset."""

from typing import cast

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
    log.info("guinea_worm.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("guinea_worm.csv"))

    # Load data from snapshot.
    df = pd.read_csv(snap.path, skiprows=2)
    df = clean_certification_table(df).reset_index(drop=True)
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

    log.info("guinea_worm.end")


def clean_certification_table(df: pd.DataFrame) -> pd.DataFrame:
    df.columns.values[0] = "country"
    df.columns.values[24] = "year_certified"
    df.year_certified = df.year_certified.str.replace(r"Countries certified in", "", regex=True)

    df = df.replace(
        {
            "year_certified": {
                "Countries at precertification stage": "Pre-certification",
                "Countries currently endemic for dracunculiasis": "Endemic",
                "Countries not known to have dracunculiasis but yet to be certified": "Pending surveillance",
            }
        }
    )

    return df

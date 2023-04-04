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


def run(dest_dir: str) -> None:
    log.info("flu_vaccine_policy.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("flu_vaccine_policy.xlsx")

    # Load data from snapshot.
    df = pd.read_excel(snap.path)
    # Drop the last row
    df = df[:-1]
    # Drop extraneous columns
    df = df.drop(columns=["ISO_3_CODE", "WHO_REGION", "INDCODE", "INDCAT_DESCRIPTION", "INDSORT"])
    df = df.rename(columns={"COUNTRYNAME": "country", "YEAR": "year"})
    df = pd.pivot(df, index=["country", "year"], columns="DESCRIPTION", values="VALUE")
    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb.update_metadata_from_yaml(paths.metadata_path, "flu_vaccine_policy")
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("flu_vaccine_policy.end")

"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("flunet.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("flunet.csv")

    # Load data from snapshot.
    df = pd.read_csv(snap.path)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb = tb.rename(columns={"country_area_territory": "country"})

    # Convert messy columns to string.
    # for col in ("aother_subtype_details", "parainfluenza", "otherrespvirus"):
    for col in ("aother_subtype_details", "other_respvirus_details"):
        ix = tb[col].notnull()
        tb.loc[ix, col] = tb.loc[ix, col].astype("str")

    # Clean up columns.
    # tb["iso_week"] = tb["iso_week"].replace({"NOTDEFINED": None}).astype(float).astype("Int64")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("flunet.end")

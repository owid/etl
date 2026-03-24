"""Load a snapshot and create a meadow dataset."""

import pandas as pd
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
    tb = snap.read_csv(underscore=True)
    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = tb.rename(columns={"country_area_territory": "country"})

    # Fix object-dtype columns caused by pandas mixed-type chunking or dirty data.
    # Columns that are mostly numeric (e.g. ah5 with stray 'RSV' strings) → to_numeric with coerce.
    # Genuinely text columns (country names, comments, etc.) → str.
    for col in tb.columns[tb.dtypes == "object"]:
        numeric = pd.to_numeric(tb[col], errors="coerce")
        total_nonnull = tb[col].notna().sum()
        if total_nonnull > 0 and numeric.notna().sum() / total_nonnull >= 0.5:
            tb[col] = numeric
        else:
            ix = tb[col].notnull()
            tb.loc[ix, col] = tb.loc[ix, col].astype("str")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("flunet.end")

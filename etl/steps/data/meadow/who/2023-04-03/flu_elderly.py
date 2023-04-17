"""Load a snapshot and create a meadow dataset.

- Load snapshot
- Drop the last line as it is a download relic
- Select out the necessary columns and remove rows which are NA for the 'coverage' dataset as this is the main variable we are interested in
- Rename in accordance with etl norms e.g. 'country' and 'year'
- Create Table and Dataset
"""

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
    log.info("flu_elderly.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("flu_elderly.xlsx")

    # Load data from snapshot.
    df = pd.read_excel(snap.path)
    # Drop the last line as it is just NAs
    df = df[:-1]

    # Subsetting to just the columns we want to use and also dropping rows where the coverage is NA as this is the main variable are interested in
    df = df[["NAME", "YEAR", "DOSES", "COVERAGE"]].dropna(subset="COVERAGE")

    # Rename inline with etl norms
    df = df.rename(columns={"NAME": "country", "YEAR": "year"}).set_index(["country", "year"], verify_integrity=True)

    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb.update_metadata_from_yaml(paths.metadata_path, "flu_elderly")
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("flu_elderly.end")

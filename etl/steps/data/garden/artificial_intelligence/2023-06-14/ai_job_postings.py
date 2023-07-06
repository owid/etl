"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("ai_job_postings.py.start")

    #
    # Load inputs.
    #
    # Load snapshots
    snap = cast(Snapshot, paths.load_dependency("ai_job_postings.csv"))
    df = pd.read_csv(snap.path)

    df["AI Job Postings (% of All Job Postings)"] *= 100
    df.rename(columns={"Label": "country"}, inplace=True, errors="raise")

    tb = Table(df, short_name="ai_job_postings", underscore=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ai_job_postings.py.end")

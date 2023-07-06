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
    log.info("ai_companies.start")

    #
    # Load inputs.
    #
    # Load snapshots
    snap = cast(Snapshot, paths.load_dependency("ai_companies.csv"))
    snap_total = cast(Snapshot, paths.load_dependency("ai_companies_total.csv"))
    df_total = pd.read_csv(snap_total.path)
    df = pd.read_csv(snap.path)

    df.rename(columns={"Label": "country"}, inplace=True)
    df_total["country"] = "World"

    df_merged = pd.concat([df, df_total])
    df_merged.reset_index(inplace=True, drop=True)
    tb = Table(df_merged, short_name=paths.short_name, underscore=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ai_companies.end")

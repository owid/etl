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
    log.info("ai_conference_type.start")

    #
    # Load inputs.
    #
    # Load snapshots
    snap_large_conf = cast(Snapshot, paths.load_dependency("ai_large_conferences.csv"))
    snap_small_conf = cast(Snapshot, paths.load_dependency("ai_small_conferences.csv"))
    snap_total_conf = cast(Snapshot, paths.load_dependency("ai_conferences_total.csv"))

    df_small = pd.read_csv(snap_small_conf.path)
    df_large = pd.read_csv(snap_large_conf.path)
    df_total = pd.read_csv(snap_total_conf.path)

    df_total["Conference"] = "Total"
    df_small_tot = pd.concat([df_small, df_total], axis=0, join="inner").reset_index(drop=True)
    df_all = pd.concat([df_small_tot, df_large], axis=0, join="inner").reset_index(drop=True)

    df_all["Number of Attendees (in Thousands)"] = (1000 * df_all["Number of Attendees (in Thousands)"]).round()

    tb = Table(df_all, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=snap_total_conf.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ai_conference_type.end")

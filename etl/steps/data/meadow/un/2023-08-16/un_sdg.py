"""Load a snapshot and create a meadow dataset."""

import re

from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("un_sdg.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot. Reading via `snap.read_feather` attaches the snapshot's
    # origin to every column, so origins propagate through meadow → garden → grapher
    # without per-step plumbing.
    snap = paths.load_snapshot("un_sdg.feather")
    tb = snap.read_feather()

    log.info("un_sdg.load_and_clean")
    tb = load_and_clean(tb)
    log.info("Size of dataframe", rows=tb.shape[0], colums=tb.shape[1])
    tb = tb.reset_index(drop=True).drop(columns="index")
    tb = tb.underscore()
    tb.metadata.short_name = paths.short_name
    ds = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)
    ds.save()
    log.info("un_sdg.end")


def load_and_clean(tb: Table) -> Table:
    log.info("un_sdg.reading_in_original_data")

    # removing values that aren't numeric e.g. Null and N values
    tb = tb.dropna(subset=["Value"])
    tb = tb.dropna(subset=["TimePeriod"], how="all")
    tb = tb.loc[pr.to_numeric(tb["Value"], errors="coerce").notnull()]
    tb = tb.rename(columns={"GeoAreaName": "Country", "TimePeriod": "Year"})
    tb = tb.rename(columns=lambda k: re.sub(r"[\[\]]", "", k))
    return tb

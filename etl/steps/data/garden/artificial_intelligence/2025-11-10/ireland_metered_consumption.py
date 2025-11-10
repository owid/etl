"""Garden step for Ireland Data Centres Metered Electricity Consumption.

This step processes the meadow data and creates a table with proper time series format
with separate indicators for data centres, other customers, and total consumption.
"""

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    """Create garden dataset from meadow dataset."""
    log.info("ireland_metered_consumption.start")

    # Load inputs
    ds_meadow = paths.load_dataset("ireland_metered_consumption")
    tb = ds_meadow.read("ireland_metered_consumption")

    origin = tb["value"].metadata.origins[0]
    # Pivot to get consumption categories as columns
    tb = tb.pivot_table(
        index=["date", "country"],
        columns="electricity_consumption",
        values="value",
    ).reset_index()
    # Clean column names
    tb.columns.name = None
    tb.columns = [col.lower().replace(" ", "_") for col in tb.columns]

    tb["data_centres_pct"] = (tb["data_centres"] / tb["all_metered_electricity_consumption"]) * 100

    # Add metadata
    for col in tb.columns:
        tb[col].metadata.origins = [origin]
    # Format table
    tb = tb.format(["country", "date"])

    # Save outputs
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )
    ds_garden.save()

    log.info("ireland_metered_consumption.end")

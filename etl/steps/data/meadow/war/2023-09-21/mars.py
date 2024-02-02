"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("mars.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("war_mars.xls")

    # Read excel
    tb = snap.read(sheet_name="ProjectMarsV1.1")

    #
    # Process data.
    #
    # Change short_name (war_mars -> mars)
    tb.m.short_name = paths.short_name
    # Ensure correct dtypes
    for col in ["startdate", "enddate"]:
        tb[col] = pd.to_datetime(tb[col])
    tb["mic_qc"] = tb["mic_qc"].replace(" ", pd.NA).astype("Int64")

    # Drop NaNs
    tb = tb.dropna(how="all")

    # Set index
    tb = tb.set_index("id", verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata, check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("mars.end")

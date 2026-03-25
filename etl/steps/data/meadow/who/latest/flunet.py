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

    # Some columns (e.g. ah5, anotsubtypable) have mixed types because pandas parsed them as
    # numeric in some CSV chunks but encountered stray strings like 'RSV' or 'ES' in others.
    # Detect these by checking for actual float/int objects inside an object-dtype column,
    # then coerce the stray strings to NaN. Pure text columns are left as str.
    KNOWN_STRAY_STRINGS = {"RSV", "ES"}
    for col in tb.columns[tb.dtypes == "object"]:
        if tb[col].dropna().apply(lambda x: isinstance(x, (int, float))).any():
            stray = {
                v for v in tb[col] if isinstance(v, str) and pd.isna(pd.to_numeric(v, errors="coerce"))
            } - KNOWN_STRAY_STRINGS
            if stray:
                raise ValueError(f"Column '{col}' contains unexpected non-numeric strings: {stray}")
            tb[col] = pd.to_numeric(tb[col], errors="coerce")
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

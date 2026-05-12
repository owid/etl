"""Load a snapshot and create a meadow dataset."""

import numpy as np

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Retrieve raw data from snapshot.
    snap = paths.load_snapshot("postnatal_care.csv")
    tb = snap.read()

    # Drop rows with no Series Code (footer / blank rows in the source CSV).
    tb = tb[tb["Series Code"].notna()]

    # Clean and transform data.
    tb = clean_data(tb)

    # Improve table format.
    tb = tb.underscore().format(["country", "year"], short_name=paths.short_name)

    # Save outputs.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()


def clean_data(tb):
    tb = tb.drop(columns=["Country Code", "Series Name", "Series Code"])

    cols = tb.columns[1:].str[:4].tolist()
    tb.columns = ["country"] + cols
    tb = tb.replace("..", np.nan)
    tb = tb.melt(id_vars="country", value_vars=cols)
    tb = tb.rename(columns={"variable": "year", "value": "postnatal_care_coverage"})
    tb["year"] = tb["year"].astype(int)
    return tb

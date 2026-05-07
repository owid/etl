"""Tiny example showing that a snapshot can be generated as a DataFrame."""

import pandas as pd
from owl.catalog import export, load_snapshot
from owl.dataset import Dataset
from owl.snapshot import Snapshot


@Snapshot
def generated_data():
    """A small DataFrame generated directly in Python and snapshotted as Parquet."""
    return pd.DataFrame(
        {
            "country": ["World", "World", "World"],
            "year": [2020, 2021, 2022],
            "value": [1, 2, 3],
        }
    )


@Dataset
def dataframe_snapshot_example(generated_data: Snapshot):
    tb = load_snapshot(generated_data)
    return export(tb)

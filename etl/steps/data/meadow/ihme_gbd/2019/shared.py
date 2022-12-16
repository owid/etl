from typing import List

import pandas as pd
import pyarrow.compute as pc
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from pyarrow import feather

from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.rename(
            columns={
                "location_name": "country",
                "location": "country",
                "val": "value",
                "measure_name": "measure",
                "sex_name": "sex",
                "age_name": "age",
                "cause_name": "cause",
                "metric_name": "metric",
            },
            errors="ignore",
        )
        .drop(
            columns=["measure_id", "location_id", "sex_id", "age_id", "cause_id", "metric_id"],
            errors="ignore",
        )
        .drop_duplicates(subset=["measure", "sex", "age", "cause", "metric", "year"])
    )


def read_and_clean_data(local_file: str) -> pd.DataFrame:
    """Reading the entire data at once and cleaning consumes too much memory (drop_duplicates
    is the culprit). So we read the data in chunks and clean each chunk separately."""
    arrow_table = feather.read_table(local_file)

    if "metric_name" in arrow_table.column_names:
        partition = "metric_name"
    elif "metric" in arrow_table.column_names:
        partition = "metric"
    else:
        partition = ""

    if partition:
        dfs: List[pd.DataFrame] = []
        for partition_name in arrow_table[partition].unique().to_pylist():
            dfs.append(clean_data(arrow_table.filter(pc.equal(arrow_table[partition], partition_name)).to_pandas()))
        return pd.concat(dfs)
    else:
        return clean_data(arrow_table.to_pandas())


def run_wrapper(dataset: str, metadata_path: str, namespace: str, version: str, dest_dir: str) -> None:
    # retrieve raw data from walden
    snap = Snapshot(f"{NAMESPACE}/{version}/{dataset}.csv")
    local_file = str(snap.path)

    df = read_and_clean_data(str(local_file))

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)
    ds.metadata.version = "2019"
    ds.metadata.title = ds.metadata.title + " - " + ds.metadata.description

    # create table with metadata from dataframe
    table_metadata = TableMeta(
        short_name=ds.metadata.short_name,
        title=ds.metadata.title,
        description=snap.metadata.description,
    )
    tb = Table(df, metadata=table_metadata)

    # underscore all table columns
    tb = underscore_table(tb)

    ds.metadata.update_from_yaml(metadata_path, if_source_exists="replace")
    tb.update_metadata_from_yaml(metadata_path, f"{dataset}")
    tb = tb.reset_index()
    # add table to a dataset
    ds.add(tb)

    # finally save the dataset
    ds.save()

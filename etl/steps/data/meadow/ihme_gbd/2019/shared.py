from pathlib import Path

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog

from etl.steps.data.converters import convert_walden_metadata


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
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
    ).drop(
        columns=["measure_id", "location_id", "sex_id", "age_id", "cause_id", "metric_id"],
        errors="ignore",
    )


def run_wrapper(dataset: Path, metadata_path: Path, namespace: Path, version: Path, dest_dir: str) -> None:
    # retrieve raw data from walden
    walden_ds = WaldenCatalog().find_one(namespace=namespace, short_name=dataset, version=version)
    local_file = walden_ds.ensure_downloaded()
    df = pd.read_feather(local_file)

    # clean and transform data
    df = clean_data(df)

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = "2019"
    ds.metadata.title = ds.metadata.title + " - " + ds.metadata.description

    # create table with metadata from dataframe
    table_metadata = TableMeta(
        short_name=ds.metadata.short_name,
        title=ds.metadata.title,
        description=walden_ds.description,
    )
    tb = Table(df, metadata=table_metadata)

    # underscore all table columns
    tb = underscore_table(tb)

    ds.metadata.update_from_yaml(metadata_path, if_source_exists="replace")
    tb.update_metadata_from_yaml(metadata_path, f"{dataset}")

    # add table to a dataset
    ds.add(tb)

    # finally save the dataset
    ds.save()

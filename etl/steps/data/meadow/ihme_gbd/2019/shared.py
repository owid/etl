from typing import List

import pandas as pd
import pyarrow.compute as pc
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog
from pyarrow import feather

from etl.steps.data.converters import convert_walden_metadata


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
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
    df = df.drop(
        columns=["measure_id", "location_id", "sex_id", "age_id", "cause_id", "metric_id", "upper", "lower"],
        errors="ignore",
    )
    df = df.astype(
        {
            "measure": "category",
            "country": "category",
            "sex": "category",
            "age": "category",
            "cause": "category",
            "metric": "category",
            "year": "int",
            "value": "float32",
        }
    )
    df = df = df.drop(df[(df.measure.isin(["Prevalence", "Incidence"])) & (df.metric == "Percent")].index)
    # df = df.groupby(["measure", "sex", "cause", "metric"])
    return df


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


def fix_percent(df: pd.DataFrame) -> pd.DataFrame:
    """
    IHME doesn't seem to be consistent with how it stores percentages.
    If the maximum percent value for any cause of death is less than or equal 1,
    it indicates all values are 100x too small and we need to multiply values by 100
    """
    if "Percent" in df["metric"].unique():
        if max(df["value"][df["metric"] == "Percent"]) <= 1:
            subset_percent = df["metric"] == "Percent"
            df.loc[subset_percent, "value"] *= 100
            # df["value"][(df["metric"] == "Percent")] = df["value"][(df["metric"] == "Percent")] * 100
    return df


def run_wrapper(dataset: str, metadata_path: str, namespace: str, version: str, dest_dir: str) -> None:
    # retrieve raw data from walden
    walden_ds = WaldenCatalog().find_one(namespace=namespace, short_name=dataset, version=version)
    local_file = walden_ds.ensure_downloaded()
    tb = read_and_clean_data(local_file)
    tb = tb.drop_duplicates()
    tb = fix_percent(tb)
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
    tb = Table(tb, metadata=table_metadata)

    # underscore all table columns
    tb = underscore_table(tb)

    ds.metadata.update_from_yaml(metadata_path, if_source_exists="replace")
    # tb.update_metadata_from_yaml(metadata_path, f"{dataset}")
    tb.reset_index(drop=True, inplace=True)
    # add table to a dataset
    ds.add(tb)

    # finally save the dataset
    ds.save()

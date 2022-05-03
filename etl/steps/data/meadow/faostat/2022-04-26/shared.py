"""Shared functions for steps in this version.

"""

import os
import tempfile
import zipfile

import pandas as pd
from owid.catalog import Dataset, Table, utils
from owid.walden import Catalog

from etl.steps.data.converters import convert_walden_metadata


def load_data(local_path: str) -> pd.DataFrame:
    # Unzip data into a temporary folder.
    with tempfile.TemporaryDirectory() as temp_dir:
        z = zipfile.ZipFile(local_path)
        z.extractall(temp_dir)
        (filename,) = list(filter(lambda x: "(Normalized)" in x, os.listdir(temp_dir)))

        # Load data from main file.
        data = pd.read_csv(os.path.join(temp_dir, filename), encoding="latin-1")

    return data


def run_sanity_checks(data: pd.DataFrame) -> None:
    df = data.copy()

    # Check nulls
    df.isnull().any()

    x = df.groupby(["Element", "Unit"])["Element Code"].nunique()
    if (x > 1).any():
        raise ValueError("Element-Unit not unique!")


def prepare_output_table(data: pd.DataFrame) -> pd.DataFrame:
    df = data.copy()

    df = df.drop(columns=["Year Code"])
    df = df.set_index(
        ["Area Code", "Item Code", "Element Code", "Year", "Flag"],
        verify_integrity=True,
    )

    return df


def generate_dataset(dest_dir: str, namespace: str, dataset_short_name: str) -> None:
    # Fetch latest walden dataset.
    walden_ds = Catalog().find_latest(
        namespace=namespace, short_name=dataset_short_name
    )

    # Initialise meadow dataset.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.short_name = dataset_short_name
    ds.save()

    # Load and prepare data.
    data = load_data(walden_ds.local_path)

    # Run sanity checks.
    run_sanity_checks(data=data)

    # Prepare output data.
    data = prepare_output_table(data=data)

    # Add tables to dataset.
    t = Table(data)
    t.metadata.short_name = dataset_short_name
    ds.add(utils.underscore_table(t))

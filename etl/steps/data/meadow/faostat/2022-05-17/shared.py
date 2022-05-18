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

    # Check that column "Year Code" is identical to "Year", and can therefore be dropped.
    error = "Column 'Year Code' does not coincide with column 'Year'."
    if df["Year"].dtype == int:
        # In most cases, columns "Year Code" and "Year" are simply the year.
        assert (df["Year Code"] == df["Year"]).all(), error
    else:
        # Sometimes (e.g. for dataset fs) there are year ranges (e.g. with "Year Code" 20002002 and "Year" "2000-2002").
        assert (df["Year Code"] == df["Year"].str.replace("-", "").astype(int)).all(), error

    # Check that there is only one element-unit for each element code.
    error = "Multiple element-unit for the same element code."
    assert (df.groupby(["Element", "Unit"])["Element Code"].nunique() == 1).all(), error


def prepare_output_table(data: pd.DataFrame) -> pd.DataFrame:
    df = data.copy()

    df = df.drop(columns=["Year Code"])

    # Set index columns depending on what columns are available in the dataframe.
    index_columns = list({"Area Code", "Item Code", "Element Code", "Year"} & set(df.columns))
    if df.duplicated(subset=index_columns).any():
        print(f"WARNING: Index has duplicated keys.")
    df = df.set_index(index_columns)

    return df


def run(dest_dir: str) -> None:
    # Assume dest_dir is a path to the step that needs to be run, e.g. "faostat_qcl", and fetch namespace and dataset
    # short name from that path.
    dataset_short_name = os.path.basename(dest_dir)
    namespace = dataset_short_name.split("_")[0]

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

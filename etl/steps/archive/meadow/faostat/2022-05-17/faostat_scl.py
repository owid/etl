"""FAOSTAT meadow step for faostat_scl dataset.

In the original data item_code seems to be mistaken with cpc_code.
For example, item "Wheat" in the data has item code 111, but in the metadata, "Wheat" has item code 15 (and cpc code
111). This does not affect the data values, but if we wanted to merge this dataset with another one using item code,
we would get wrong results. Also, descriptions fetched from the metadata would be wrong for this dataset.
Here, we rename item_code to cpc_code and join with the metadata to get the true item codes.

Apart from this issue, the rest of the processing of the dataset is identical to all other datasets.

"""

from pathlib import Path

import pandas as pd
from owid.catalog import Dataset, Table, utils
from owid.walden import Catalog
from shared import (
    LATEST_VERSIONS_FILE,
    NAMESPACE,
    load_data,
    prepare_output_data,
    run_sanity_checks,
)

from etl.paths import DATA_DIR
from etl.steps.data.converters import convert_walden_metadata


def fix_items(data: pd.DataFrame, metadata: Dataset) -> pd.DataFrame:
    """Add the true item codes to the data, extracted from the metadata.

    Parameters
    ----------
    data : pd.DataFrame
        Data for faostat_scl.
    metadata : catalog.Dataset
        Global metadata dataset.

    Returns
    -------
    data_fixed : pd.DataFrame
        Original data after replacing item_code by the true item codes.

    """
    # Get items metadata for faostat_scl dataset.
    items_metadata = metadata[f"{NAMESPACE}_scl_item"]

    # Replace item_code by cpc_code, join with items metadata for this dataset, and get the right item_codes.
    data_fixed = (
        pd.merge(
            data.reset_index(drop=True).rename(columns={"Item Code": "cpc_code"}),
            items_metadata.reset_index()[["cpc_code", "item_code"]],
            on="cpc_code",
            how="left",
        )
        .drop(columns="cpc_code")
        .rename(columns={"item_code": "Item Code"})
    )

    return data_fixed


def run(dest_dir: str) -> None:
    ####################################################################################################################
    # Common definitions.
    ####################################################################################################################

    # Assume dest_dir is a path to the step that needs to be run, e.g. "faostat_qcl", and fetch dataset short name from
    # that path.
    dataset_short_name = Path(dest_dir).name

    ####################################################################################################################
    # Load data.
    ####################################################################################################################

    # Load file of versions.
    latest_versions = pd.read_csv(LATEST_VERSIONS_FILE).set_index(["channel", "dataset"])

    # Fetch latest walden dataset.
    walden_version = latest_versions.loc["walden", dataset_short_name].item()
    walden_ds = Catalog().find_one(namespace=NAMESPACE, version=walden_version, short_name=dataset_short_name)

    # Load data.
    data = load_data(walden_ds.local_path)

    # Load metadata.
    metadata_version = latest_versions.loc["meadow", f"{NAMESPACE}_metadata"].item()
    metadata = Dataset(DATA_DIR / "meadow" / NAMESPACE / metadata_version / f"{NAMESPACE}_metadata")

    ####################################################################################################################
    # Prepare data.
    ####################################################################################################################

    # Fix issue with item codes.
    data = fix_items(data=data, metadata=metadata)

    # Run sanity checks.
    run_sanity_checks(data=data)

    ####################################################################################################################
    # Save outputs.
    ####################################################################################################################

    # Prepare output data.
    data = prepare_output_data(data=data)

    # Initialise meadow dataset.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.short_name = dataset_short_name
    ds.save()

    # Add tables to dataset.
    t = Table(data)
    t.metadata.short_name = dataset_short_name
    ds.add(utils.underscore_table(t))

"""FAOSTAT (additional) metadata dataset (originally ingested in walden using the FAOSTAT API).

Load the (additional) metadata dataset from walden, and create a meadow dataset with as many tables as domain-categories
(e.g. 'faostat_qcl_area', 'faostat_fbs_item', ...).

All categories are defined below in 'category_structure'.

"""

import json
from typing import Any, Dict

import pandas as pd
from owid.catalog import Dataset, Table, utils
from owid.walden import Catalog
from shared import LATEST_VERSIONS_FILE, NAMESPACE

from etl.steps.data.converters import convert_walden_metadata

# Name for new meadow dataset.
DATASET_SHORT_NAME = f"{NAMESPACE}_metadata"

# Define the structure of the additional metadata file.
category_structure = {
    "area": {
        "index": ["Country Code"],
        "short_name": "area",
    },
    "areagroup": {
        "index": ["Country Group Code", "Country Code"],
        "short_name": "area_group",
    },
    "element": {
        "index": ["Element Code"],
        "short_name": "element",
    },
    "flag": {
        "index": ["Flag"],
        "short_name": "flag",
    },
    "glossary": {
        "index": ["Glossary Code"],
        "short_name": "glossary",
    },
    "item": {
        "index": ["Item Code"],
        "short_name": "item",
    },
    "itemfactor": {
        "index": ["Item Group Code", "Item Code", "Element Code"],
        "short_name": "item_factor",
    },
    "itemgroup": {
        "index": ["Item Group Code", "Item Code"],
        "short_name": "item_group",
    },
    "items": {
        "index": ["Item Code"],
        "short_name": "item",
    },
    "itemsgroup": {
        "index": ["Item Group Code", "Item Code"],
        "short_name": "item_group",
    },
    "recipientarea": {
        "index": ["Recipient Country Code"],
        "short_name": "area",
    },
    "unit": {
        "index": ["Unit Name"],
        "short_name": "unit",
    },
    "year": {
        "index": ["Year Code"],
        "short_name": "year",
    },
    "year3": {
        "index": ["Year Code"],
        "short_name": "year",
    },
}


def check_that_category_structure_is_well_defined(md: Dict[str, Any]) -> None:
    """Check that metadata content is consistent with category_structure (defined above).

    If that is not the case, it is possible that the content of metadata has changed, and therefore category_structure
    may need to be edited.

    Parameters
    ----------
    md : dict
        Raw FAOSTAT (additional) metadata of all datasets.

    """
    for dataset in list(md):
        for category in category_structure:
            category_indexes = category_structure[category]["index"]
            if category in md[dataset]:
                category_metadata = md[dataset][category]["data"]
                for entry in category_metadata:
                    for category_index in category_indexes:
                        error = (
                            f"Index {category_index} not found in {category} for {dataset}. "
                            f"Consider redefining category_structure."
                        )
                        assert category_index in entry, error


def run(dest_dir: str) -> None:
    # Load file of versions.
    latest_versions = pd.read_csv(LATEST_VERSIONS_FILE).set_index(["channel", "dataset"])

    # Load FAOSTAT (additional) metadata dataset from walden.
    walden_latest_version = latest_versions.loc["walden", DATASET_SHORT_NAME].item()
    walden_ds = Catalog().find_one(
        namespace=NAMESPACE,
        version=walden_latest_version,
        short_name=DATASET_SHORT_NAME,
    )

    local_file = walden_ds.ensure_downloaded()
    with open(local_file) as _local_file:
        additional_metadata = json.load(_local_file)

    # Check that metadata content is consistent with category_structure (defined above).
    check_that_category_structure_is_well_defined(md=additional_metadata)

    # Create new meadow dataset, importing its metadata from walden.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.short_name = DATASET_SHORT_NAME
    ds.save()
    # Create a new table within the dataset for each domain-record (e.g. 'faostat_qcl_item').
    for domain in additional_metadata:
        for category in list(additional_metadata[domain]):
            json_data = additional_metadata[domain][category]["data"]
            df = pd.DataFrame.from_dict(json_data)
            if len(df) > 0:
                df.set_index(
                    category_structure[category]["index"],
                    verify_integrity=True,
                    inplace=True,
                )
                t = Table(df)
                t.metadata.short_name = f'{NAMESPACE}_{domain.lower()}_{category_structure[category]["short_name"]}'
                ds.add(utils.underscore_table(t))

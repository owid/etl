"""Additional FAOSTAT metadata from the FAO API.

"""

import json
from typing import Any, Dict

import pandas as pd
from owid.walden import Catalog
from owid.catalog import Dataset, Table, utils

from etl.steps.data.converters import convert_walden_metadata

NAMESPACE = "faostat"
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
    for dataset in list(md):
        for category in category_structure:
            category_indexes = category_structure[category]["index"]
            if category in md[dataset]:
                category_metadata = md[dataset][category]["data"]
                for entry in category_metadata:
                    for category_index in category_indexes:
                        error = (
                            f"Index {category_index} not found in {category} for {dataset}. "
                            "Redefine category_structure."
                        )
                        assert category_index in entry, error


def run(dest_dir: str) -> None:
    # Load walden dataset.
    walden_ds = Catalog().find_latest(
        namespace=NAMESPACE, short_name=DATASET_SHORT_NAME
    )
    local_file = walden_ds.ensure_downloaded()

    # Load and restructure
    with open(local_file) as _local_file:
        additional_metadata = json.load(_local_file)

    # Check that category_structure is well defined.
    check_that_category_structure_is_well_defined(md=additional_metadata)

    # Create new meadow dataset, importing metadata from walden.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.short_name = DATASET_SHORT_NAME
    ds.save()
    # Create a new table within the dataset for each domain-record.
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

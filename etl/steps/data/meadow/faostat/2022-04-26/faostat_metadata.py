"""Additional FAOSTAT metadata.

Several FAO datasets need identifiers that come from the FAO API. Here we reconstruct them from a snapshot.

"""

import json

import pandas as pd
from owid.walden import Catalog
from owid.catalog import Dataset, Table, utils

from etl.steps.data.converters import convert_walden_metadata

NAMESPACE = "faostat"
DATASET_SHORT_NAME = f"{NAMESPACE}_metadata"

# Define the structure of the additional metadata file.
category_structure = {
    "itemgroup": {
        "index": ["Item Group Code", "Item Code"],
        "short_name": "item",
    },
    # Category "itemsgroup" seems to only exist for qcl.
    "itemsgroup": {
        "index": ["Item Group Code", "Item Code"],
        "short_name": "item",
    },
    "area": {
        "index": ["Country Code"],
        "short_name": "area",
    },
    "element": {
        "index": ["Element Code"],
        "short_name": "element",
    },
    "unit": {
        "index": ["Unit Name"],
        "short_name": "unit",
    },
    "flag": {
        "index": ["Flag"],
        "short_name": "flag",
    },
}


def run(dest_dir: str) -> None:
    # Load walden dataset.
    walden_ds = Catalog().find_latest(
        namespace=NAMESPACE, short_name=DATASET_SHORT_NAME
    )
    local_file = walden_ds.ensure_downloaded()

    # Load and restructure
    with open(local_file) as _local_file:
        additional_metadata = json.load(_local_file)

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
                t.metadata.short_name = f'meta_{domain.lower()}_{category_structure[category]["short_name"]}'
                ds.add(utils.underscore_table(t))

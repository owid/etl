"""Additional FAO metadata.

Several FAO datasets need identifiers that come from the FAO API. Here we reconstruct them from a snapshot.

"""

import json
from copy import deepcopy

import pandas as pd
from owid.walden import Catalog
from owid.catalog import Dataset, Table, utils

from etl.steps.data.converters import convert_walden_metadata

NAMESPACE = "faostat"
DATASET_SHORT_NAME = f"{NAMESPACE}_metadata"
# Define the structure of the additional metadata file.
default_domain_records = [
    {
        "category": "itemgroup",
        "index": ["Item Group Code", "Item Code"],
        "short_name": "item",
    },
    {
        "category": "area",
        "index": ["Country Code"],
        "short_name": "area",
    },
    {
        "category": "element",
        "index": ["Element Code"],
        "short_name": "element",
    },
    {
        "category": "unit",
        "index": ["Unit Name"],
        "short_name": "unit",
    }]
additional_metadata_paths = {domain: deepcopy(default_domain_records) for domain in ['FBS', 'FBSH', 'QCL', 'RL']}
# Fix different spelling of QCL "itemsgroup" to the more common "itemgroup".
additional_metadata_paths['QCL'][0]['category'] = 'itemsgroup'


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
    # Create a new table within the dataset for each domain-record.
    for domain in additional_metadata:
        for record in additional_metadata_paths[domain]:
            json_data = additional_metadata[domain][record["category"]]["data"]
            df = pd.DataFrame.from_dict(json_data)
            if len(df) > 0:
                df.set_index(record["index"], verify_integrity=True, inplace=True)
                t = Table(df)
                t.metadata.short_name = f'meta_{domain.lower()}_{record["short_name"]}'
                ds.add(utils.underscore_table(t))

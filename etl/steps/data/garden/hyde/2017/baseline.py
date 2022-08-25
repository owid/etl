#
#  Hyde baseline 2017
#
#  Harmonize countries in the Hyde baseline.
#

import json
from pathlib import Path
from typing import Dict, cast

from owid.catalog import Dataset

from etl.paths import DATA_DIR

MAPPING_FILE = Path(__file__).with_suffix(".mapping.json")


def run(dest_dir: str) -> None:
    harmonize_countries("meadow/hyde/2017/baseline", dest_dir)


def harmonize_countries(source_ds_path: str, dest_dir: str) -> None:
    """
    Harmonize the country field of every table in the source dataset.
    """
    # this is deliberately done in a generic way as a demonstration
    source_ds = Dataset(DATA_DIR / source_ds_path)
    mapping = load_mapping()

    ds = Dataset.create_empty(dest_dir, metadata=source_ds.metadata)

    for table in source_ds:
        # harmonize this specific table
        names = table.index.names
        table = table.reset_index()

        # harmonize countries; fail by design if a match is not found
        table["country"] = table.country.apply(lambda c: mapping[c])

        table.set_index(names, inplace=True)

        ds.add(table)

    ds.save()


def load_mapping() -> Dict[str, str]:
    country_mapping_file = MAPPING_FILE
    with open(country_mapping_file) as istream:
        mapping = json.load(istream)

    return cast(Dict[str, str], mapping)

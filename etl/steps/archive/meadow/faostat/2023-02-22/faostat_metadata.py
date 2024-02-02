"""Load FAOSTAT (additional) metadata (snapshot ingested using the API) and create a meadow faostat_metadata dataset.

The resulting meadow dataset has as many tables as domain-categories ('faostat_qcl_area', 'faostat_fbs_item', ...).

All categories are defined below in 'category_structure'.

"""

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from owid.catalog import Table
from owid.datautils.io import load_json
from shared import CURRENT_DIR, NAMESPACE

from etl.helpers import PathFinder, create_dataset

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
    # Specific for faostat_fa.
    "recipientarea": {
        "index": ["Recipient Country Code"],
        "short_name": "area",
    },
    "unit": {
        "index": ["Unit Name"],
        "short_name": "unit",
    },
    # Specific for faostat_fa.
    "year": {
        "index": ["Year Code"],
        "short_name": "year",
    },
    # Specific for faostat_fs.
    "year3": {
        "index": ["Year Code"],
        "short_name": "year",
    },
    "years": {
        "index": ["Year Code"],
        "short_name": "year",
    },
    # Specific for faostat_wcad.
    "yearwca": {
        "index": ["Year Code"],
        "short_name": "year",
    },
    # Specific for faostat_gn.
    "sources": {
        "index": ["Source Code"],
        "short_name": "sources",
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


def create_tables_for_all_domain_records(additional_metadata: Dict[str, Any]) -> List[Table]:
    """Create a table for each of the domain-categories (e.g. 'faostat_qcl_item').

    Parameters
    ----------
    additional_metadata : Dict[str, Any]
        FAOSTAT additional metadata.

    Returns
    -------
    tables: List[Table]
        List of tables, each one corresponding to a specific domain-category.

    """
    # Create a new table for each domain-category (e.g. 'faostat_qcl_item').
    tables = []
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
                table_short_name = f'{NAMESPACE}_{domain.lower()}_{category_structure[category]["short_name"]}'
                table = Table(df, short_name=table_short_name)
                tables.append(table)

    return tables


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Fetch the dataset short name from dest_dir.
    dataset_short_name = Path(dest_dir).name

    # Define path to current step file.
    current_step_file = (CURRENT_DIR / dataset_short_name).with_suffix(".py")

    # Get paths and naming conventions for current data step.
    paths = PathFinder(current_step_file.as_posix())

    # Load snapshot.
    snapshot = paths.load_dependency(short_name=dataset_short_name + ".json", channel="snapshot")
    additional_metadata = load_json(snapshot.path)

    #
    # Process data.
    #
    # Run sanity checks.
    check_that_category_structure_is_well_defined(md=additional_metadata)

    # Create a new table for each domain-record (e.g. 'faostat_qcl_item').
    tables = create_tables_for_all_domain_records(additional_metadata=additional_metadata)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir=dest_dir, tables=tables, default_metadata=snapshot.metadata)
    ds_meadow.save()

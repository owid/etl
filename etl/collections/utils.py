import re
from collections import defaultdict
from typing import Any, Dict, List, Set, Union

from owid.catalog import Dataset, Table

from etl.paths import DATA_DIR


def records_to_dictionary(records, key: str):
    """Transform: [{key: ..., a: ..., b: ...}, ...] -> {key: {a: ..., b: ...}, ...}."""

    dix = {}
    for record in records:
        assert key in record, f"`{key}` not found in record: {record}!"
        dix[record[key]] = {k: v for k, v in record.items() if k != key}

    return dix


def get_tables_by_name_mapping(dependencies: Set[str]) -> Dict[str, List[Table]]:
    """Dictionary mapping table short name to table object.

    Note that the format is {"table_name": [tb], ...}. This is because there could be collisions where multiple table names are mapped to the same table (e.g. two datasets could have a table with the same name).
    """
    tb_name_to_tb = defaultdict(list)

    for dep in dependencies:
        ## Ignore non-grapher dependencies
        if not re.match(r"^(data|data-private)://grapher/", dep):
            continue

        uri = re.sub(r"^(data|data-private)://", "", dep)
        ds = Dataset(DATA_DIR / uri)
        for table_name in ds.table_names:
            tb_name_to_tb[table_name].append(ds.read(table_name, load_data=False))

    return tb_name_to_tb


def extract_catalog_path(indicator_raw):
    "Indicator spec can come either as a plain string, or a dictionary."
    if isinstance(indicator_raw, str):
        return indicator_raw
    elif isinstance(indicator_raw, dict):
        assert "catalogPath" in indicator_raw
        return indicator_raw["catalogPath"]
    else:
        raise ValueError(f"Unexpected indicator property type: {indicator_raw}")


################################################
# DEPRECATE
################################################

DIMENSIONS = ["y", "x", "size", "color"]


def get_indicators_in_view(view):
    """Get the list of indicators in use in a view.

    It returns the list as a list of records:

    [
        {
            "path": "data://path/to/dataset#indicator",
            "dimension": "y"
        },
        ...
    ]

    TODO: This is being called twice, maybe there is a way to just call it once. Maybe if it is an attribute of a class?
    """
    indicators_view = []
    # Get indicators from dimensions
    for dim in DIMENSIONS:
        if dim in view["indicators"]:
            indicator_raw = view["indicators"][dim]
            if isinstance(indicator_raw, list):
                assert dim == "y", "Only `y` can come as a list"
                indicators_view += [
                    {
                        "path": extract_catalog_path(ind),
                        "dimension": dim,
                    }
                    for ind in indicator_raw
                ]
            else:
                indicators_view.append(
                    {
                        "path": extract_catalog_path(indicator_raw),
                        "dimension": dim,
                    }
                )
    return indicators_view

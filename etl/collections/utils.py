import re
from collections import defaultdict
from typing import Any, Dict, List, Set, Union

from owid.catalog import Dataset, Table

from etl.paths import DATA_DIR

DIMENSIONS = ["y", "x", "size", "color"]


def records_to_dictionary(records, key: str):
    """Transform: [{key: ..., a: ..., b: ...}, ...] -> {key: {a: ..., b: ...}, ...}."""

    dix = {}
    for record in records:
        assert key in record, f"`{key}` not found in record: {record}!"
        dix[record[key]] = {k: v for k, v in record.items() if k != key}

    return dix


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


def expand_catalog_paths(view: Dict[Any, Any], tables_by_name: Dict[str, List[Table]]) -> Dict[Any, Any]:
    """Expand catalog paths in views to full dataset URIs.

    This function updates the given configuration dictionary in-place by modifying the dimension ('y', 'x', 'size', 'color') entries under "indicators" in each view. If an entry does not contain a '/',
    it is assumed to be a table name that must be expanded to a full dataset URI based on
    the provided dependencies.

    NOTE: Possible improvements for internal function `_expand`:
        - we should make this function a bit more robust when checking the URIs.
        - currently we only allow for 'table#indicator' format. We should also allow for other cases that could be useful in the event of name collisions, e.g. 'dataset/indicator#subindicator'.

    Args:
        config (dict): Configuration dictionary containing views.
        tables_by_name (Dict[str, List[Table]]): Mapping of table short names to tables.
    """

    def _expand_catalog_path(indicator: Union[str, Dict[str, str]]) -> Union[str, Dict[str, str]]:
        """Return same indicator, but with complete catalog path."""

        def _expand(indicator: str):
            assert "#" in indicator, f"Missing '#' in indicator! '{indicator}'"

            # Complete dataset URI
            if "/" in indicator:
                return indicator
            # table#indicator format
            else:
                indicator_split = indicator.split("#")

                # Check format is actually table#indicator
                assert (len(indicator_split) == 2) & (
                    indicator_split[0] != ""
                ), f"Expected 'table#indicator' format. Instead found {indicator}"

                # Check table is in any of the datasets!
                assert (
                    indicator_split[0] in tables_by_name
                ), f"Table name `{indicator_split[0]}` not found in dependency tables! Available tables are: {', '.join(tables_by_name.keys())}"

                # Check table name to table mapping is unique
                assert (
                    len(tables_by_name[indicator_split[0]]) == 1
                ), f"There are multiple dependencies (datasets) with a table named {indicator_split[0]}. Please use the complete dataset URI in this case."

                # Check dataset in table metadata is not None
                tb = tables_by_name[indicator_split[0]][0]
                assert tb.m.dataset is not None, f"Dataset not found for table {indicator_split[0]}"

                # Build URI
                return tb.m.dataset.uri + "/" + indicator

        # Expand catalog path if it's a string
        if isinstance(indicator, str):
            return _expand(indicator)
        # Expand catalog path if it's a dictionary
        elif isinstance(indicator, dict):
            assert "catalogPath" in indicator, "Expected 'catalogPath' key in indicator dictionary"
            indicator["catalogPath"] = _expand(indicator["catalogPath"])
            return indicator

    # Update indicators for each dimension
    for dim in DIMENSIONS:
        if dim in view["indicators"]:
            if isinstance(view["indicators"][dim], list):
                view["indicators"][dim] = [_expand_catalog_path(dim) for dim in view["indicators"][dim]]
            else:
                view["indicators"][dim] = _expand_catalog_path(view["indicators"][dim])

    # Update indicators from sortColumnSlug
    if "config" in view:
        if "sortColumnSlug" in view["config"]:
            view["config"]["sortColumnSlug"] = _expand_catalog_path(view["config"]["sortColumnSlug"])

    # Update indicators from map.columnSlug
    if "config" in view:
        if "map" in view["config"]:
            if "columnSlug" in view["config"]["map"]:
                view["config"]["map"]["columnSlug"] = _expand_catalog_path(view["config"]["map"]["columnSlug"])

    return view


def extract_catalog_path(indicator_raw):
    "Indicator spec can come either as a plain string, or a dictionary."
    if isinstance(indicator_raw, str):
        return indicator_raw
    elif isinstance(indicator_raw, dict):
        assert "catalogPath" in indicator_raw
        return indicator_raw["catalogPath"]
    else:
        raise ValueError(f"Unexpected indicator property type: {indicator_raw}")

from typing import Any, Dict, List, Union

from owid.catalog import Table

from etl.collections.model import DIMENSIONS


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

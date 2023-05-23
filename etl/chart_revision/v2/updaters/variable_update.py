"""Logic to update charts when there are updates on variables.

These functions are used when there are updates on variables. They are used in the chart revision process.
"""


from collections import Counter
from typing import Any, Dict, List, Literal, Optional, Set, Tuple, cast
from urllib.error import URLError

from sqlmodel import Session, select
from structlog import get_logger

import etl.grapher_model as gm
from backport.datasync.data_metadata import variable_data_df_from_s3
from etl.chart_revision.v2.base import ChartUpdater
from etl.chart_revision.v2.schema import (
    get_schema_chart_config,
    validate_chart_config_and_remove_defaults,
    validate_chart_config_and_set_defaults,
)
from etl.db import get_engine

log = get_logger()


class ChartVariableUpdater(ChartUpdater):
    """Handle chart updates when there are updates on variables."""

    def __init__(self, variable_mapping: Dict[int, int], schema: Optional[Dict[str, Any]] = None) -> None:
        """Constructor.

        Parameters
        ----------
        variable_mapping : Dict[int, int]
            Mapping between old and new variable IDs.
        """
        # Variable mapping dictionary: Old variable ID -> New variable ID
        self.variable_mapping = variable_mapping
        # Lists with variable IDs (old, new and all)
        self.variable_ids_old = set(variable_mapping.keys())
        self.variable_ids_new = set(variable_mapping.values())
        self.variable_ids_all = self.variable_ids_old | self.variable_ids_new
        # Variable metadata (e.g. min and max years)
        self.__variable_meta = None
        # Set schema
        if schema is not None:
            self.schema = schema
        else:
            self.schema = get_schema_chart_config()

    @property
    def variable_meta(self) -> Dict[int, Any]:
        """Variable metadata."""
        if self.__variable_meta is None:
            raise ValueError(
                "Variable metadata is not set. `variable_metadata` is set when `find_charts_to_be_updated` is called."
            )
        return self.__variable_meta

    def find_charts_to_be_updated(self) -> List[gm.Chart]:
        """Find charts that use the variables that are being updated.

        Also, it gets metadata for variables in use in the charts.

        Returns
        -------
        List[gm.Chart]
            List of charts that use the variables that are being updated.
        """
        # Get charts to be updated
        charts = find_charts_from_variable_ids(self.variable_ids_old)
        # Get metadata for variables in use in the charts
        self.__variable_meta = self._get_variable_metadata(charts)
        return charts

    def _get_variable_metadata(self, charts: List[gm.Chart]) -> Dict[int, Any]:
        """Get variable metadata.

        We need to get metadata for variables being updated (old and new), but also for variables that are not being updated, but are used in the charts.
        This is because we need to know the min and max years for all variables used in the charts to update the chart time configuration.
        """
        # Get variable IDs from all variables (dimensions x and y) in charts that will be updated
        variable_ids = set()
        for chart in charts:
            variable_ids |= set(c["variableId"] for c in chart.config["dimensions"] if c["property"] in {"x", "y"})
        log.info(
            f"variable_update: getting variable metadata of {len(variable_ids)} variables from {len(charts)} charts"
        )
        # Combine with new variables (not yet used in charts)
        variable_ids = list(variable_ids | set(self.variable_ids_new))

        # Get metadata of variables from S3 (try twice)
        log.info("0")
        try:
            log.info(1)
            df = variable_data_df_from_s3(get_engine(), variable_ids=[int(v) for v in variable_ids], workers=10)
        except URLError:
            try:
                log.info(12)
                df = variable_data_df_from_s3(get_engine(), variable_ids=[int(v) for v in variable_ids], workers=10)
            except URLError:
                log.info(2)
                raise URLError(
                    "Could not connect to S3. If this persists, please report in #tech-issues channel on slack."
                )

        # Reshape metadata, we want a dictionary!
        variable_meta = (
            df.groupby("variableId").year.agg(["min", "max"]).rename(columns={"min": "minYear", "max": "maxYear"})
        )
        variable_meta = variable_meta.to_dict(orient="index")
        # raise URLError("Could not connect to S3. If this persists, please report in #tech-issues channel on slack.")
        return variable_meta

    def run(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Run the chart variable updater."""
        log.info("variable_update: updating configuration")
        # Validate the configuration of the chart and add default values
        config_new = validate_chart_config_and_set_defaults(config, self.schema)
        # Update map configuration
        config_new = update_config_map(
            config_new,
            self.variable_mapping,
            self.variable_meta,
            config_new["dimensions"][0]["variableId"],
        )
        # Update config time
        config_new = update_config_time(config_new, self.variable_mapping, self.variable_meta)
        # Update dimensions
        config_new = update_config_dimensions(config_new, self.variable_mapping)
        # Update sort
        config_new = update_config_sort(config_new, self.variable_mapping)
        # Validate the  configuration of the chart and remove default values (if any)
        config_new = validate_chart_config_and_remove_defaults(config_new, self.schema)
        return config_new


def find_charts_from_variable_ids(variable_ids: Set[int]) -> List[gm.Chart]:
    """Retrieve charts that use the given variables from their IDs.

    TODO: Currently we make two calls to the database. Can probably be reduced to one.

    Parameters
    ----------
    variable_ids : List[int]
        List with variable IDs.

    Returns
    -------
    List[gm.Chart]
        List of charts using the specified variables.
    """
    log.info(f"variable_update: finding charts using old variables ({len(variable_ids)} variables)")
    with Session(get_engine()) as session:
        # Find IDs of charts that use the given variable IDs
        chart_ids = (
            session.exec(select(gm.ChartDimensions.chartId).where(gm.ChartDimensions.variableId.in_(variable_ids)))  # type: ignore
            .unique()
            .all()
        )
        # Retrieve charts from a given list of chart IDs
        return session.exec(select(gm.Chart).where(gm.Chart.id.in_(chart_ids))).all()  # type: ignore


def update_config_map(
    config: Dict[str, Any],
    variable_mapping: Dict[int, int],
    variable_meta: Dict[int, Any],
    variable_id_default_for_map: int,
) -> Dict[str, Any]:
    """Update map config.

    This mainly involves updating the slider in the map view.

    Parameters
    ----------
    config : Dict[str, Any]
        Configuration of the chart. It is assumed that no changed has occured under the property `map`.
    variable_mapping : Dict[int, int]
        Mapping from old to new variable IDs.
    variable_meta : Dict[int, int]
        Variable metadata. Includes min and max years.
    variable_id_default_for_map : int
        ID of the first variable in the old chart. Usually found at config["dimensions"][0]["variableId"]

    Returns
    -------
    Dict[str, Any]
        Updated chart configuration.
    """
    log.info("variable_update: updating map config")
    # Proceed only if chart uses map
    if config["hasMapTab"]:
        log.info("variable_update: chart uses map")
        # Get map_variable_id
        map_var_id = config["map"].get(
            "variableId", variable_id_default_for_map
        )  # chart.config["dimensions"][0]["variableId"]
        # Proceed only if variable ID used for map is in variable_mapping (i.e. needs update)
        if map_var_id in variable_mapping:
            # Get and set new map variable ID in the chart config
            map_var_id_new = variable_mapping[map_var_id]
            config["map"]["variableId"] = map_var_id_new
            # Get year ranges from old and new variables
            year_range_new_min = variable_meta[map_var_id_new]["minYear"]
            year_range_new_max = variable_meta[map_var_id_new]["maxYear"]

            # Set year slider to new value based on new variable's year range
            # - If old time was set to "latest", keep it as it is.
            # - If old time is greater than maximum value in new range, set to 'latest'.
            # - If old time is lower than minimum value in new range, set to the min of the new range.
            # - If old time was set to a particular year, keep it as it is.
            current_time = config["map"]["time"]
            if isinstance(current_time, int):
                if current_time >= year_range_new_max:
                    print(f"{current_time} >= {year_range_new_max}")
                    config["map"]["time"] = "latest"
                elif current_time <= year_range_new_min:
                    print(f"{current_time} <= {year_range_new_min}")
                    config["map"]["time"] = year_range_new_min
    return config


def update_config_time(
    config: Dict[str, Any],
    variable_mapping: Dict[int, int],
    variable_meta: Dict[int, Any],
) -> Dict[str, Any]:
    """Update time config.

    Parameters
    ----------
    config : Dict[str, Any]
        Configuration of the chart. It is assumed that no changed has occured under the property `map`.
    variable_mapping : Dict[int, int]
        Mapping from old to new variable IDs.
    variable_meta : Dict[int, Any]
        Variable metadata. Includes min and max years.

    Returns
    -------
    Dict[str, Any]
        Updated chart configuration.
    """
    log.info("variable_update: updating time config")
    chart_type = config["type"]
    dimensions_used = Counter(
        dim["property"] for dim in config["dimensions"] if dim["property"] not in ["color", "size"]
    )
    if chart_type == "LineChart":
        # Sanity check
        if ("x" in dimensions_used) | (dimensions_used["y"] < 1):
            log.error(
                f"{chart_type} {config['id']} cannot have an `x` dimension and must have at least one `y` dimension"
            )
        # Update config
        config = _update_config_time_specific_chart(config, variable_mapping, variable_meta, "union", ["y"])

    elif chart_type == "ScatterPlot":
        # Sanity check
        if (dimensions_used.get("x", 0) > 1) | (dimensions_used["y"] != 1):
            log.error(
                f"{chart_type} {config['id']} cannot have more than one `x` dimension and must have exactly one `y` dimension"
            )
        # Update config
        config = _update_config_time_specific_chart(config, variable_mapping, variable_meta, "intersect", ["x", "y"])
    elif chart_type == "TimeScatter":
        # Sanity check
        if (dimensions_used.get("x", 0) > 1) | (dimensions_used["y"] != 1):
            log.error(
                f"{chart_type} {config['id']} cannot have more than one `x` dimension and must have exactly one `y` dimension"
            )
        # Update config
        config = _update_config_time_specific_chart(config, variable_mapping, variable_meta, "intersect", ["x", "y"])

    elif chart_type == "StackedArea":
        # Sanity check
        if ("x" in dimensions_used) | (dimensions_used["y"] < 1):
            log.error(
                f"{chart_type} {config['id']} cannot have an `x` dimension and must have at least one `y` dimension"
            )
        # Update config
        config = _update_config_time_specific_chart(config, variable_mapping, variable_meta, "intersect", ["y"])
    elif chart_type == "StackedBar":
        # Sanity check
        if ("x" in dimensions_used) | (dimensions_used["y"] < 1):
            log.error(
                f"{chart_type} {config['id']} cannot have an `x` dimension and must have at least one `y` dimension"
            )
        # Update config
        config = _update_config_time_specific_chart(config, variable_mapping, variable_meta, "intersect", ["y"])

    elif chart_type == "DiscreteBar":
        # Sanity check
        if ("x" in dimensions_used) | (dimensions_used["y"] < 1):
            log.error(
                f"{chart_type} {config['id']} cannot have an `x` dimension and must have at least one `y` dimension"
            )
        # Update config
        config = _update_config_time_specific_chart(config, variable_mapping, variable_meta, "single", ["y"])
    elif chart_type == "StackedDiscreteBar":
        # Sanity check
        if ("x" in dimensions_used) | (dimensions_used["y"] < 1):
            log.error(
                f"{chart_type} {config['id']} cannot have an `x` dimension and must have at least one `y` dimension"
            )
        # Update config
        config = _update_config_time_specific_chart(config, variable_mapping, variable_meta, "single", ["y"])

    elif chart_type == "Marimekko":
        # Sanity check
        if (dimensions_used.get("x", 0) > 1) | (dimensions_used["y"] < 1):
            log.error(
                f"{chart_type} {config['id']} cannot have more than one `x` dimension and must have at least one `y` dimension"
            )
        # Update config
        config = _update_config_time_specific_chart(config, variable_mapping, variable_meta, "single", ["y"])

    elif chart_type == "SlopeChart":
        # Sanity check
        if ("x" in dimensions_used) | (dimensions_used["y"] != 1):
            log.error(
                f"{chart_type} {config['id']} cannot have an `x` dimension and must have exactly one `y` dimension"
            )
        # Update config
        config = _update_config_time_specific_chart(config, variable_mapping, variable_meta, "union", ["y"])

    else:
        log.error(f"Unknown chart type `{chart_type}`")
    return config


def update_config_dimensions(config: Dict[str, Any], variable_mapping: Dict[int, int]) -> Dict[str, Any]:
    """Update dimensions in the chart config.

    Parameters
    ----------
    config : Dict[str, Any]
        Configuration of the chart. It is assumed that no changed has occured under the property `dimensions`.
    variable_mapping : Dict[int, int]
        Mapping from old to new variable IDs.

    Returns
    -------
    Dict[str, Any]
        Updated chart configuration.
    """
    log.info("variable_update: updating dimensions")
    # Update dimensions field
    for dimension in config["dimensions"]:
        if dimension["variableId"] in variable_mapping:
            dimension["variableId"] = variable_mapping[dimension["variableId"]]
    return config


def update_config_sort(config: Dict[str, Any], variable_mapping: Dict[int, int]) -> Dict[str, Any]:
    """Update sort in the chart config.

    There are three fields that deal with the sorting of bars in bar charts and marimekko.

        - sortBy: sorting criterium ('column', 'total' or 'entityName')
        - sortColumnSlug: if sortBy is "column", this is the slug of the column to sort by.
        - sortOrder: "asc" or "desc"

    This fields should remain the same. However, when the sorting criterium is set to 'column', the column slug (i.e. variable ID)
    may have changed due to dataset update.

    Parameters
    ----------
    config : Dict[str, Any]
        Configuration of the chart. It is assumed that no changed has occured under the property `sort`.
    variable_mapping : Dict[int, int]
        Mapping from old to new variable IDs.

    Returns
    -------
    Dict[str, Any]
        Updated chart configuration.
    """
    log.info("variable_update: updating sortBy property")
    if "sortBy" in config:
        if config["sortBy"] == "column":
            assert "sortColumnSlug" in config, "sortBy is 'column' but sortColumnSlug is not defined!"
            var_old_id = config["sortColumnSlug"]
            config["sortColumnSlug"] = str(variable_mapping.get(var_old_id, var_old_id))
    return config


def _update_config_time_specific_chart(
    config: Dict[str, Any],
    variable_mapping: Dict[int, int],
    variable_meta: Dict[int, Any],
    range_mode: Literal["intersect", "union", "single"],
    properties: List[str],
) -> Dict[str, Any]:
    """Update selected time.

    - Min/Max times set to either earliest/latest are left unchanged.
    - Min/Max times set to a particular numerical value are left unchanged.
    - Timeline is only changed if selected time falls out of range.

    We should encourage chart editors to set limits to either earliest/latest if they want it to be always updated.
    """
    _, ranges_new = _get_time_ranges(config, variable_mapping, variable_meta, properties)
    if range_mode == "intersect":
        assert properties is not None, "Need to specify properties for intersect mode"
        range_new_min, range_new_max = intersect_range(ranges_new)
    elif range_mode == "union":
        assert properties is not None, "Need to specify properties for union mode"
        range_new_min, range_new_max = union_range(ranges_new)
    elif range_mode == "single":
        range_new_min, range_new_max = union_range(ranges_new)
        if config["maxTime"] not in ["latest", "earliest"]:
            config["maxTime"] = min(max(config["maxTime"], range_new_min), range_new_max)
        return config
    else:
        raise ValueError(f"Unknown range mode `{range_mode}`")

    # Only update if minTime/maxTime is not set to latest/earliest and falls out of range
    if (config["minTime"] not in ["latest", "earliest"]) & (config["maxTime"] not in ["latest", "earliest"]):
        log.info(f"variable_update: updating time range of chart. Old: {config['minTime']} -> {config['maxTime']}.")
        config["minTime"] = min(max(config["minTime"], range_new_min), range_new_max)
        config["maxTime"] = min(max(config["maxTime"], range_new_min), range_new_max)
        if config["minTime"] > config["maxTime"]:
            config["minTime"] = config["maxTime"]
        log.info(f"variable_update: updating time range of chart. New: {config['minTime']} -> {config['maxTime']}.")
    else:
        log.info(f"variable_update: keeping time range of chart. {config['minTime']} -> {config['maxTime']}.")
    return config


def _get_time_ranges(
    config: Dict[str, Any], variable_mapping: Dict[int, int], variable_meta: Dict[int, Any], properties: List[str]
) -> Tuple[List[Tuple[int, int]], List[Tuple[int, int]]]:
    """Get time ranges of variables used in the chart configuration.

    It obtains the list of ranges with the currently in use variables and the future set of variables in use.
    """
    ranges_old = []
    ranges_new = []
    # Iterate over all dimensions used in the chart, obtain their year ranges, and add to range list.
    # Also check if variable will be updated, so that the new range is added to ranges_new.
    for dim in config["dimensions"]:
        if dim["property"] in properties:
            variable_id = dim["variableId"]
            ranges_old.append([variable_meta[variable_id]["minYear"], variable_meta[variable_id]["maxYear"]])
            if variable_id in variable_mapping:
                variable_id_new = variable_mapping[variable_id]
                ranges_new.append(
                    (
                        variable_meta[variable_id_new]["minYear"],
                        variable_meta[variable_id_new]["maxYear"],
                    )
                )
    return (ranges_old, ranges_new)


def intersect_range(ranges: List[Tuple[int, int]]) -> Tuple[int, int]:
    """Get the range that is common to all ranges in `ranges`.

    It performs INTERSECT operation on the ranges.
    """
    range_min = None
    range_max = None
    for range_ in ranges:
        if range_min is None or range_[0] > range_min:
            range_min = range_[0]
        if range_max is None or range_[1] < range_max:
            range_max = range_[1]
    return cast(int, range_min), cast(int, range_max)


def union_range(ranges: List[Tuple[int, int]]) -> Tuple[int, int]:
    """Get the range that contains all ranges in `ranges`.

    It performs UNION operation on the ranges.
    """
    range_min = None
    range_max = None
    for range_ in ranges:
        if range_min is None or range_[0] < range_min:
            range_min = range_[0]
        if range_max is None or range_[1] > range_max:
            range_max = range_[1]
    return cast(int, range_min), cast(int, range_max)

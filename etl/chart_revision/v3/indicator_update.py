"""Logic to update charts when there are updates on variables.

These functions are used when there are updates on variables. They are used in the chart revision process.
"""


from copy import deepcopy
from typing import Any, Dict, List, Optional, Set

from sqlmodel import Session, select
from structlog import get_logger

import etl.grapher_model as gm
from etl.chart_revision.v3.schema import (
    fix_errors_in_schema,
    get_schema_chart_config,
    validate_chart_config_and_remove_defaults,
    validate_chart_config_and_set_defaults,
)
from etl.db import get_engine

# Logger
log = get_logger()


def find_charts_from_variable_ids(variable_ids: Set[int]) -> List[gm.Chart]:
    """Retrieve charts that use the given variables from their IDs."""
    log.info(f"variable_update: finding charts using old variables ({len(variable_ids)} variables)")
    with Session(get_engine()) as session:
        # Find IDs of charts that use the given variable IDs
        chart_ids = (
            session.exec(select(gm.ChartDimensions.chartId).where(gm.ChartDimensions.variableId.in_(variable_ids)))  # type: ignore
            .unique()
            .all()
        )
        # Retrieve charts from a given list of chart IDs
        charts = session.exec(select(gm.Chart).where(gm.Chart.id.in_(chart_ids))).all()  # type: ignore

    # some charts don't have ID in config, fix that here (should be ideally fixed in the database)
    for chart in charts:
        if "id" not in chart.config:
            log.warning(f"Chart {chart.id} does not have an ID in config.")
            chart.config["id"] = chart.id
        if "version" not in chart.config:
            log.warning(f"Chart {chart.id} does not have a version in config.")
            chart.config["version"] = 1

    return charts


def update_chart_config(
    config: Dict[str, Any],
    indicator_mapping: Dict[int, int],
    schema: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Update indicator references to the new ones.

    The chart config contains some fields that point to the indicators in use. In the attempt to migrating these, we should update all references to the new indicators.
    """
    updater = ChartVariableUpdater(indicator_mapping, schema)
    config_new = updater.run(deepcopy(config))
    return config_new


class ChartVariableUpdater:
    """Handle chart updates when there are updates on variables."""

    def __init__(
        self,
        indicator_mapping: Dict[int, int],
        schema: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Constructor.

        Parameters
        ----------
        variable_mapping : Dict[int, int]
            Mapping between old and new variable IDs.
        schema : Optional[Dict[str, Any]]
            Schema of the chart configuration. Defaults to None.
        skip_slider_check_limit : int
            If the number of variables to be updated is greater than this value, the slider range check is disabled. That is, no changes to the slider are performed.
            This is to avoid errors when updating charts with many variables. Defaults to None.
        """
        # Variable mapping dictionary: Old variable ID -> New variable ID
        self.indicator_mapping = indicator_mapping
        if schema is None:
            self.schema = get_schema_chart_config()
        self.schema = schema

    def run(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Run the chart variable updater."""
        # Fix errors in schema
        config_new = fix_errors_in_schema(config)
        # Validate config agains schema
        config_new = validate_chart_config_and_set_defaults(config_new, self.schema)
        # Update map tab
        config_new = update_chart_config_map(config_new, self.indicator_mapping)
        # Update dimensions
        config_new = update_chart_config_dimensions(config_new, self.indicator_mapping)
        # Update sorting
        config_new = update_chart_config_sort(config_new, self.indicator_mapping)
        # Validate the  configuration of the chart and remove default values (if any)
        config_new = validate_chart_config_and_remove_defaults(config_new, self.schema)
        return config_new


def update_chart_config_map(
    config: Dict[str, Any],
    indicator_mapping: Dict[int, int],
) -> Dict[str, Any]:
    """Update map config."""
    log.info("variable_update: updating map config")
    # Proceed only if chart uses map
    if config["hasMapTab"]:
        log.info("variable_update: chart uses map")
        # Get map.columnSlug
        map_var_id = config["map"].get(
            "columnSlug", config["dimensions"][0]["variableId"]
        )  # chart.config["dimensions"][0]["variableId"]
        # Proceed only if variable ID used for map is in variable_mapping (i.e. needs update)
        if map_var_id in indicator_mapping:
            # Get and set new map variable ID in the chart config
            config["map"]["columnSlug"] = str(indicator_mapping[map_var_id])
    return config


def update_chart_config_dimensions(
    config: Dict[str, Any],
    indicator_mapping: Dict[int, int],
) -> Dict[str, Any]:
    """Update dimensions in the chart config."""
    log.info("variable_update: updating dimensions")
    # Update dimensions field
    for dimension in config["dimensions"]:
        if dimension["variableId"] in indicator_mapping:
            dimension["variableId"] = indicator_mapping[dimension["variableId"]]
    return config


def update_chart_config_sort(
    config: Dict[str, Any],
    indicator_mapping: Dict[int, int],
) -> Dict[str, Any]:
    """Update sort in the chart config.

    There are three fields that deal with the sorting of bars in bar charts and marimekko.

        - sortBy: sorting criterium ('column', 'total' or 'entityName')
        - sortColumnSlug: if sortBy is "column", this is the slug of the column to sort by.
        - sortOrder: "asc" or "desc"

    This fields should remain the same. However, when the sorting criterium is set to 'column', the column slug (i.e. variable ID)
    may have changed due to dataset update.
    """
    if "sortBy" in config:
        log.info("variable_update: updating sortBy property")
        if config["sortBy"] == "column":
            assert "sortColumnSlug" in config, "sortBy is 'column' but sortColumnSlug is not defined!"
            var_old_id = config["sortColumnSlug"]
            config["sortColumnSlug"] = str(indicator_mapping.get(int(var_old_id), var_old_id))
    return config

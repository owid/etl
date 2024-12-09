"""Logic to update charts when there are updates on variables.

These functions are used when there are updates on variables. They are used in the chart revision process.
"""

from copy import deepcopy
from typing import Any, Dict, List, Set

from sqlalchemy import select
from sqlalchemy.orm import Session
from structlog import get_logger

import etl.grapher_model as gm
from etl.db import get_engine
from etl.indicator_upgrade.schema import (
    fix_errors_in_schema,
    validate_chart_config_and_remove_defaults,
    validate_chart_config_and_set_defaults,
)

# Logger
log = get_logger()


def find_charts_from_variable_ids(variable_ids: Set[int]) -> List[gm.Chart]:
    """Retrieve charts that use the given variables from their IDs."""
    log.info(f"variable_update: finding charts using old variables ({len(variable_ids)} variables)")
    with Session(get_engine()) as session:
        # Find IDs of charts that use the given variable IDs
        chart_ids = (
            session.scalars(select(gm.ChartDimensions.chartId).where(gm.ChartDimensions.variableId.in_(variable_ids)))
            .unique()
            .all()
        )
        # Retrieve charts from a given list of chart IDs
        charts = session.scalars(select(gm.Chart).where(gm.Chart.id.in_(chart_ids))).all()

    # some charts don't have ID in config, fix that here (should be ideally fixed in the database)
    for chart in charts:
        if "id" not in chart.config:
            log.warning(f"Chart {chart.id} does not have an ID in config.")
            chart.config["id"] = chart.id
        if "version" not in chart.config:
            log.warning(f"Chart {chart.id} does not have a version in config.")
            chart.config["version"] = 1

    return list(charts)


def update_chart_config(
    config: Dict[str, Any],
    indicator_mapping: Dict[int, int],
    schema: Dict[str, Any],
) -> Dict[str, Any]:
    """Update indicator references to the new ones.

    The chart config contains some fields that point to the indicators in use. In the attempt to migrating these, we should update all references to the new indicators.
    """
    updater = ChartIndicatorUpdater(indicator_mapping, schema)
    config_new = updater.run(deepcopy(config))
    return config_new


class ChartIndicatorUpdater:
    """Handle chart updates when there are updates on variables."""

    def __init__(
        self,
        indicator_mapping: Dict[int, int],
        schema: Dict[str, Any],
    ) -> None:
        """Constructor.

        Parameters
        ----------
        indicator_mapping : Dict[int, int]
            Mapping between old and new indicator IDs.
        schema : Optional[Dict[str, Any]]
            Schema of the chart configuration. Defaults to None.
        """
        # Variable mapping dictionary: Old variable ID -> New variable ID
        self.indicator_mapping = indicator_mapping
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
    # Proceed only if chart uses map and has `map` field
    if config["hasMapTab"] and "map" in config:
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

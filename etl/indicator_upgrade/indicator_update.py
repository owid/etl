"""Logic to update charts when there are updates on variables.

These functions are used when there are updates on variables. They are used in the chart revision process.
"""

from copy import deepcopy
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session
from structlog import get_logger

import etl.grapher.model as gm
from etl.db import get_engine
from etl.indicator_upgrade.schema import (
    compute_inheritance_patch,
    fix_errors_in_schema,
    prune_dimension_displays,
    validate_chart_config_and_remove_defaults,
    validate_chart_config_and_set_defaults,
)

# Logger
log = get_logger()


def find_charts_from_variable_ids(variable_ids: set[int]) -> list[gm.Chart]:
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


def update_narrative_chart_config(
    config: dict[str, Any],
    indicator_mapping: dict[int, int],
) -> dict[str, Any]:
    """Update indicator references in a narrative chart config.

    Narrative charts have a simpler config structure (just the patch).
    We only need to update the dimensions and map columnSlug.
    """
    config_new = deepcopy(config)

    # Update dimensions
    if "dimensions" in config_new:
        for dimension in config_new["dimensions"]:
            if dimension.get("variableId") in indicator_mapping:
                dimension["variableId"] = indicator_mapping[dimension["variableId"]]

    # Update map columnSlug if present
    if "map" in config_new and "columnSlug" in config_new["map"]:
        try:
            map_var_id = int(config_new["map"]["columnSlug"])
            if map_var_id in indicator_mapping:
                config_new["map"]["columnSlug"] = str(indicator_mapping[map_var_id])
        except (ValueError, TypeError):
            pass  # columnSlug might not be a numeric variable ID

    # Update sortColumnSlug if present
    if config_new.get("sortBy") == "column" and "sortColumnSlug" in config_new:
        try:
            var_old_id = int(config_new["sortColumnSlug"])
            if var_old_id in indicator_mapping:
                config_new["sortColumnSlug"] = str(indicator_mapping[var_old_id])
        except (ValueError, TypeError):
            pass

    return config_new


def collect_variable_ids_from_narrative_config(config: dict[str, Any]) -> set[int]:
    """Collect the indicator IDs referenced by a narrative chart config.

    Reads the same fields that `update_narrative_chart_config` rewrites (dimensions,
    map.columnSlug, sortColumnSlug), so callers can tell which referenced indicators a given
    mapping did *not* cover.
    """
    variable_ids: set[int] = set()

    for dimension in config.get("dimensions", []):
        if isinstance(dimension.get("variableId"), int):
            variable_ids.add(dimension["variableId"])

    if "map" in config and "columnSlug" in config["map"]:
        try:
            variable_ids.add(int(config["map"]["columnSlug"]))
        except (ValueError, TypeError):
            pass  # columnSlug might not be a numeric variable ID

    if config.get("sortBy") == "column" and "sortColumnSlug" in config:
        try:
            variable_ids.add(int(config["sortColumnSlug"]))
        except (ValueError, TypeError):
            pass

    return variable_ids


def update_chart_config(
    config: dict[str, Any],
    indicator_mapping: dict[int, int],
    schema: dict[str, Any],
    indicator_config: dict[str, Any] | None = None,
    dimension_display_baselines: dict[int, dict[str, Any]] | None = None,
    original_patch: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Update indicator references to the new ones.

    The chart config contains some fields that point to the indicators in use. In the attempt to migrating these, we should update all references to the new indicators.

    `indicator_config` is the ETL grapher config of the chart's (single) indicator, used to
    correctly strip defaults from inheritance-enabled charts -- see ChartIndicatorUpdater.run.
    `dimension_display_baselines` maps each dimension's (new) variable ID to that variable's
    own `display` metadata, used to strip the same kind of redundant defaults from
    `dimensions[*].display` -- a separate inheritance path, see prune_dimension_displays.
    `original_patch` is the chart's own stored `chart_configs.patch` (not `.full`) -- see
    ChartIndicatorUpdater.run for why this matters for inheritance-enabled charts.
    """
    is_inheritance_enabled = config.get("isInheritanceEnabled", False)
    updater = ChartIndicatorUpdater(indicator_mapping, schema, is_inheritance_enabled=is_inheritance_enabled)
    config_new = updater.run(
        deepcopy(config),
        indicator_config=indicator_config,
        dimension_display_baselines=dimension_display_baselines,
        original_patch=original_patch,
    )
    return config_new


class ChartIndicatorUpdater:
    """Handle chart updates when there are updates on variables."""

    def __init__(
        self,
        indicator_mapping: dict[int, int],
        schema: dict[str, Any],
        is_inheritance_enabled: bool = False,
    ) -> None:
        """Constructor.

        Parameters
        ----------
        indicator_mapping : Dict[int, int]
            Mapping between old and new indicator IDs.
        schema : Optional[Dict[str, Any]]
            Schema of the chart configuration. Defaults to None.
        is_inheritance_enabled : bool
            Whether chart inheritance is enabled. When True, stripping schema-default
            values naively would silently revert genuine overrides that happen to match
            the schema default but differ from the indicator's own value (e.g.
            hasMapTab=false overriding the indicator's hasMapTab=true) -- see #5911. See
            `run`'s `indicator_config` parameter for how this is now handled precisely.
        """
        # Variable mapping dictionary: Old variable ID -> New variable ID
        self.indicator_mapping = indicator_mapping
        self.schema = schema
        self.is_inheritance_enabled = is_inheritance_enabled

    def run(
        self,
        config: dict[str, Any],
        indicator_config: dict[str, Any] | None = None,
        dimension_display_baselines: dict[int, dict[str, Any]] | None = None,
        original_patch: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Run the chart variable updater.

        Parameters
        ----------
        indicator_config : Optional[Dict[str, Any]]
            The ETL grapher config of the chart's own indicator (only meaningful for
            inheritance-enabled, single-indicator charts). When provided, defaults are
            stripped by comparing against this actual inherited baseline (see
            `compute_inheritance_patch`) rather than against the generic schema default,
            so genuine overrides that happen to equal the schema default -- but differ
            from the indicator's value -- are preserved. When inheritance is enabled but
            no `indicator_config` is available (e.g. multi-indicator charts, where the
            merge semantics across indicators aren't handled here), we conservatively fall
            back to keeping every field, exactly as before #5911's fix, to avoid regressing
            it.
        dimension_display_baselines : Optional[Dict[int, Dict[str, Any]]]
            Maps each dimension's (new) variable ID to that variable's own `display`
            metadata, used to strip the same kind of redundant defaults from
            `dimensions[*].display` -- a separate inheritance path from `indicator_config`
            above (see `prune_dimension_displays`). Applied regardless of
            `is_inheritance_enabled`, since dimension display always inherits from the
            variable's own display metadata.
        original_patch : Optional[Dict[str, Any]]
            The chart's own stored `chart_configs.patch` (not `.full`). `config` itself is
            normally `chart.config`, which SQLAlchemy builds from `.full` -- the fully
            resolved config, with every value the chart *inherits* from its indicator
            already merged in. If we tracked "explicitly set" paths from `config` directly,
            an inherited field the chart never touched (e.g. its title, pulled from the
            *old* indicator) would be misread as an explicit override, and compared against
            the *new* indicator's (possibly different) value in `compute_inheritance_patch`
            -- pinning a stale value as a fake chart-level override instead of letting the
            chart continue inheriting the new indicator's current value. Using the real
            stored patch instead means only genuine overrides are ever considered explicit.
            Falls back to `config` itself (the old, incorrect-for-this-purpose behavior)
            when not provided, so existing callers that only have `chart.config` available
            keep working -- just without this protection.
        """
        # Fix errors in schema
        config_new = fix_errors_in_schema(config)
        # Remember which paths were explicitly present before defaults get filled in below.
        # Prefer the chart's actual stored patch over `config` itself -- see the
        # `original_patch` parameter docstring above for why this distinction matters.
        original_config = fix_errors_in_schema(original_patch) if original_patch is not None else config_new
        # Validate config agains schema
        config_new = validate_chart_config_and_set_defaults(config_new, self.schema)
        # Update map tab
        config_new = update_chart_config_map(config_new, self.indicator_mapping)
        # Update dimensions
        config_new = update_chart_config_dimensions(config_new, self.indicator_mapping)
        # Update sorting
        config_new = update_chart_config_sort(config_new, self.indicator_mapping)
        # Validate the configuration of the chart and remove default values (if any).
        if self.is_inheritance_enabled:
            if indicator_config is not None:
                config_new = compute_inheritance_patch(
                    config_new, indicator_config, self.schema, original_config=original_config
                )
            # else: no indicator config available -- keep everything (see docstring above).
        else:
            config_new = validate_chart_config_and_remove_defaults(config_new, self.schema)
        if dimension_display_baselines:
            config_new = prune_dimension_displays(config_new, dimension_display_baselines, original_config)
        return config_new


def update_chart_config_map(
    config: dict[str, Any],
    indicator_mapping: dict[int, int],
) -> dict[str, Any]:
    """Update map config."""
    log.info("variable_update: updating map config")
    # Proceed only if chart uses map and has `map` field
    if config["hasMapTab"] and "map" in config:
        log.info("variable_update: chart uses map")
        # Get map.columnSlug. It's stored as a string in the config (unlike
        # dimensions[*].variableId, which is an int), so it must be cast before comparing
        # against indicator_mapping's int keys -- otherwise the `in` check below always
        # fails silently, leaving the map tab pinned to the old variable even though
        # every other part of the chart (dimensions, etc.) gets upgraded correctly.
        map_var_id = config["map"].get(
            "columnSlug", config["dimensions"][0]["variableId"]
        )  # chart.config["dimensions"][0]["variableId"]
        try:
            map_var_id = int(map_var_id)
        except (TypeError, ValueError):
            return config
        # Proceed only if variable ID used for map is in variable_mapping (i.e. needs update)
        if map_var_id in indicator_mapping:
            # Get and set new map variable ID in the chart config
            config["map"]["columnSlug"] = str(indicator_mapping[map_var_id])
    return config


def update_chart_config_dimensions(
    config: dict[str, Any],
    indicator_mapping: dict[int, int],
) -> dict[str, Any]:
    """Update dimensions in the chart config."""
    log.info("variable_update: updating dimensions")
    # Update dimensions field
    for dimension in config["dimensions"]:
        if dimension["variableId"] in indicator_mapping:
            dimension["variableId"] = indicator_mapping[dimension["variableId"]]
    return config


def update_chart_config_sort(
    config: dict[str, Any],
    indicator_mapping: dict[int, int],
) -> dict[str, Any]:
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

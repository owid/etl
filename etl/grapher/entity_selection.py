"""Find charts whose pinned entity selection has no data.

Grapher silently drops a selected entity that has no data in the chart's indicators. A chart pinned to an entity that a
dataset update renamed (e.g. `African Region (WHO)` after the data switched to `Africa (WHO)`) therefore renders with
series missing, and nothing fails. The helpers here make that detectable: chart-diff flags edited charts with dead pins,
and the grapher step flags dataset upserts that would kill pins of existing charts.
"""

import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

import requests
from owid.catalog import Table
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from structlog import get_logger

from etl import config
from etl.db import production_or_master_env, read_sql
from etl.exceptions import StagingServerUnavailable
from etl.http import session as http_session

log = get_logger()

# Indicator ids mapped to the names of entities with data, or None when the indicator's metadata could not be
# fetched. Unknown availability must never be read as "no entities", or every pin would look dead.
EntitiesByVariable = dict[int, set[str] | None]


def chart_variable_ids(chart_config: dict[str, Any]) -> list[int]:
    """Indicators whose entities a chart's selection is drawn from: its y dimensions, or all dimensions if it has none."""
    dimensions = chart_config.get("dimensions") or []
    y_ids = [d["variableId"] for d in dimensions if d.get("property") == "y"]
    return y_ids or [d["variableId"] for d in dimensions]


def renders_selection(chart_config: dict[str, Any]) -> bool:
    """Whether the chart's selection decides which entities it draws.

    Map-only charts (`chartTypes: []`) never render the selection, and in scatter plots and Marimekko charts it only
    highlights entities among all of them. A scatter can lack the `type` field, so it's detected by its x dimension.
    """
    if not chart_config.get("selectedEntityNames") or chart_config.get("chartTypes") == []:
        return False
    chart_types = chart_config.get("chartTypes") or [chart_config.get("type")]
    has_x = any(d.get("property") == "x" for d in chart_config.get("dimensions") or [])
    return not has_x and not {"ScatterPlot", "Marimekko"} & set(chart_types)


def dead_selected_entities(chart_config: dict[str, Any], entities_by_variable: EntitiesByVariable) -> list[str] | None:
    """Entities in `selectedEntityNames` that have no data in any of the chart's indicators.

    Returns None when availability is unknown for one of the chart's indicators.
    """
    if not renders_selection(chart_config):
        return []
    selected = chart_config["selectedEntityNames"]
    variable_ids = chart_variable_ids(chart_config)
    if not variable_ids:
        return []
    with_data: set[str] = set()
    for variable_id in variable_ids:
        entities = entities_by_variable.get(variable_id)
        if entities is None:
            return None
        with_data |= entities
    return [entity for entity in selected if entity not in with_data]


def fetch_entities_with_data(
    variable_ids: list[int], metadata_url: Callable[[int], str], workers: int = 20
) -> EntitiesByVariable:
    """Fetch the entities with data of each indicator from its metadata.json in the data API."""

    def fetch(variable_id: int) -> set[str] | None:
        try:
            response = http_session.get(metadata_url(variable_id), timeout=60)
            response.raise_for_status()
            return {entity["name"] for entity in response.json()["dimensions"]["entities"]["values"]}
        except (requests.RequestException, KeyError, ValueError) as e:
            log.warning("entity_selection.fetch_failed", variable_id=variable_id, error=str(e))
            return None

    unique_ids = sorted(set(variable_ids))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        return dict(zip(unique_ids, executor.map(fetch, unique_ids)))


def format_dead_entities(dead: list[str]) -> str:
    return ", ".join(f"'{entity}'" for entity in dead)


class PinnedEntitiesRemovedError(Exception):
    pass


@dataclass
class ChartPins:
    chart_id: int
    slug: str
    config: dict[str, Any]


class UpsertPinsCheck:
    """Catch a dataset upsert that removes entities which existing charts pin.

    Without a version bump the chart configs never change, so chart-diff doesn't see this; the grapher step is the only
    place it is visible. A pin counts as killed when it has data in production, but not once production's entities of
    this dataset's indicators are replaced by the upserted ones. Indicators of other datasets on the same chart keep
    their production entities, so a step is never blamed for pins another dataset killed. Comparing against production
    rather than the previous run keeps the check failing on every staging build until the charts are fixed, while pins
    already dead in production stay out of scope (they aren't this upsert's doing). An indicator that disappears from
    the dataset is left to the ghost-variable check, which already fails while charts use it.

    Usage: `prepare()` before upserting (on production the upsert overwrites the data it compares against), `add()`
    each upserted indicator, then `run()`.
    """

    def __init__(self, engine: Engine, dataset_id: int):
        self.engine = engine
        self.dataset_id = dataset_id
        self.charts: list[ChartPins] = []
        # Variable ids of the charts' indicators in this environment, mapped to their catalog paths.
        self.catalog_paths: dict[int, str] = {}
        self.tracked_paths: set[str] = set()
        self.entities_in_production: EntitiesByVariable = {}
        # Entities with data of the indicators being upserted, by catalog path.
        self.upserted_entities: dict[str, set[str]] = {}

    def prepare(self) -> None:
        # SUBSET runs are partial development iterations (e.g. `SUBSET='France,Germany'` in the data step), so most
        # pins would look removed. Leaving `charts` empty skips the check.
        if config.SUBSET:
            return
        q = """
        SELECT c.id, cc.slug, cc.config
        FROM charts c
        JOIN chart_configs cc ON cc.id = c.configId
        WHERE c.id IN (
            SELECT cd.chartId FROM chart_dimensions cd JOIN variables v ON v.id = cd.variableId
            WHERE v.datasetId = %(dataset_id)s
        )
        AND cc.config ->> '$.isPublished' = 'true'
        AND JSON_LENGTH(cc.config, '$.selectedEntityNames') > 0
        """
        df = read_sql(q, engine=self.engine, params={"dataset_id": self.dataset_id})
        charts = [
            ChartPins(chart_id=int(row.id), slug=str(row.slug), config=json.loads(str(row.config)))
            for row in df.itertuples()
        ]
        self.charts = [chart for chart in charts if renders_selection(chart.config)]
        variable_ids = sorted({v for chart in self.charts for v in chart_variable_ids(chart.config)})
        if not variable_ids:
            return

        df = read_sql(
            "SELECT id, catalogPath FROM variables WHERE id IN %(ids)s",
            engine=self.engine,
            params={"ids": tuple(variable_ids)},
        )
        self.catalog_paths = {int(row.id): str(row.catalogPath) for row in df.itertuples() if row.catalogPath}
        self.tracked_paths = set(self.catalog_paths.values())
        if not self.catalog_paths:
            return

        # Production ids of the same indicators (ids minted on staging don't exist in production, or name another one)
        production_env = production_or_master_env()
        try:
            df = read_sql(
                "SELECT id, catalogPath FROM variables WHERE catalogPath IN %(paths)s",
                engine=production_env.engine,
                params={"paths": tuple(self.catalog_paths.values())},
            )
        except (OperationalError, StagingServerUnavailable) as e:
            # E.g. local development without access to production; availability stays unknown, so nothing is flagged
            log.warning("entity_selection.production_unreachable", error=str(e))
            return
        production_ids = {str(row.catalogPath): int(row.id) for row in df.itertuples()}
        entities = fetch_entities_with_data(list(production_ids.values()), production_env.indicator_metadata_url)
        self.entities_in_production = {
            variable_id: entities[production_ids[path]]
            for variable_id, path in self.catalog_paths.items()
            if path in production_ids
        }

    def add(self, catalog_path: str, table: Table) -> None:
        if catalog_path in self.tracked_paths:
            self.upserted_entities[catalog_path] = set(table.index.get_level_values("entityName"))

    def run(self) -> None:
        if not self.charts:
            return

        # Entities with data after the upsert: production's, with this dataset's upserted indicators swapped in
        entities_after: EntitiesByVariable = dict(self.entities_in_production)
        for variable_id, path in self.catalog_paths.items():
            if path in self.upserted_entities:
                entities_after[variable_id] = self.upserted_entities[path]

        killed = []
        for chart in self.charts:
            dead_after = dead_selected_entities(chart.config, entities_after)
            dead_in_production = dead_selected_entities(chart.config, self.entities_in_production)
            # Unknown either side (fetch failed, or a chart on indicators production doesn't have) isn't a finding
            if dead_after is None or dead_in_production is None:
                continue
            if newly_dead := [entity for entity in dead_after if entity not in dead_in_production]:
                killed.append((chart, newly_dead))
        if not killed:
            return

        lines = [
            f"- {chart.slug} ({config.OWID_ENV.chart_admin_site(chart.chart_id)}): {format_dead_entities(dead)}"
            for chart, dead in killed
        ]
        message = (
            f"Upserting dataset {self.dataset_id} removes entities that {len(killed)} published chart(s) pin in their "
            "selection, so these charts would render without them:\n"
            + "\n".join(lines)
            + "\nRename the pins to the entities the data now uses (or remove them) in the chart admin, or keep the "
            "old entity names in the data."
        )
        if config.ENV == "production":
            message += (
                "\nIf this ran for a merged PR whose charts were fixed on staging, chart-sync (which runs after ETL) "
                "delivers the fixes and there is nothing to do."
            )
        # In production, ETL runs before chart-sync, so failing here would block the very sync that delivers the
        # fixed charts. Report instead.
        if config.ENV == "production":
            from slack_sdk.errors import SlackClientError

            from etl.slack_helpers import send_slack_message

            log.error("entity_selection.pinned_entities_removed", message=message)
            # A Slack outage must not fail the step either, for the same reason (OSError covers network failures)
            try:
                send_slack_message(channel="#data-architecture-github", message=message)
            except (SlackClientError, OSError) as e:
                log.error("entity_selection.slack_failed", error=str(e))
        else:
            raise PinnedEntitiesRemovedError(message)

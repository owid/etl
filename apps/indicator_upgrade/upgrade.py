"""CLI functions for upgrading indicators and charts."""

import difflib
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import click
import pandas as pd
from sqlalchemy.orm import Session
from structlog import get_logger

import etl.grapher.model as gm
from apps.chart_sync.admin_api import AdminAPI, AdminAPIError
from apps.wizard.utils.cached import get_grapher_user
from apps.wizard.utils.db import WizardDB
from etl.config import OWID_ENV
from etl.db import get_engine, read_sql
from etl.files import get_schema_from_url
from etl.indicator_upgrade.indicator_update import (
    collect_variable_ids_from_narrative_config,
    find_charts_from_variable_ids,
    update_chart_config,
    update_narrative_chart_config,
)

# Default number of parallel workers
DEFAULT_MAX_WORKERS = 5

log = get_logger()


def get_affected_charts_cli(indicator_mapping: dict[int, int]) -> list[gm.Chart]:
    """Get affected charts for CLI (without Streamlit dependencies)."""
    log.info("Finding affected charts...")
    charts = find_charts_from_variable_ids(set(indicator_mapping.keys()))
    log.info(f"Found {len(charts)} affected charts")
    return charts


def _fetch_single_indicator_config(chart_config: dict, indicator_mapping: dict[int, int]) -> dict | None:
    """Fetch the ETL grapher config of a chart's own (primary) indicator.

    This is the baseline an inheritance-enabled chart inherits from, used by
    `compute_inheritance_patch` to strip only the fields that would resolve to the same
    value anyway (see #5911 and its follow-up fix).

    For a chart with more than one dimension (e.g. a `color` or `size` variable besides
    `y`), title/subtitle/map/etc. inheritance is driven by the `y` dimension's own
    indicator, not the secondary ones -- so we use that one specifically rather than
    giving up. Falls back to the first dimension with a variable ID if there's no `y`
    dimension (e.g. scatter plots with only x/y numeric axes and no property named "y").

    Returns `{}` -- not None -- when the (single) indicator has no `grapherConfigETL` at
    all: that means inheritance provides nothing, so the effective baseline *is* just the
    generic schema defaults, which `compute_inheritance_patch` handles safely (as opposed
    to None, which triggers the separate, more conservative "no baseline known at all"
    keep-everything fallback in `ChartIndicatorUpdater.run`).
    Returns None only when the chart has no dimension with a variable ID to look up at all.
    """
    dimensions = chart_config.get("dimensions", [])
    y_dims = [dim for dim in dimensions if dim.get("property") == "y" and dim.get("variableId")]
    candidates = y_dims or [dim for dim in dimensions if dim.get("variableId")]
    if not candidates:
        return None
    variable_id = indicator_mapping.get(candidates[0]["variableId"], candidates[0]["variableId"])

    df = read_sql(
        "SELECT cc.full FROM variables v JOIN chart_configs cc ON cc.id = v.grapherConfigIdETL WHERE v.id = %(vid)s",
        engine=get_engine(),
        params={"vid": int(variable_id)},
    )
    if df.empty:
        return {}
    full = df.iloc[0]["full"]
    return json.loads(full) if isinstance(full, (str, bytes)) else full


def _fetch_dimension_display_baselines(chart_config: dict, indicator_mapping: dict[int, int]) -> dict[int, dict] | None:
    """Fetch each dimension's (new, remapped) variable's own `display` metadata.

    Used by `prune_dimension_displays` to strip `dimensions[*].display` fields that are
    redundant with the variable's own display settings -- a separate inheritance path
    from `_fetch_single_indicator_config`'s grapherConfigETL-based one (dimension display
    inherits from `variables.display`, not from the chart's own indicator config).
    """
    variable_ids = {dim.get("variableId") for dim in chart_config.get("dimensions", []) if dim.get("variableId")}
    if not variable_ids:
        return None
    new_variable_ids = {int(indicator_mapping.get(vid, vid)) for vid in variable_ids}

    df = read_sql(
        f"SELECT id, display FROM variables WHERE id IN ({','.join(str(v) for v in sorted(new_variable_ids))})",
        engine=get_engine(),
    )
    if df.empty:
        return None
    return {
        int(row["id"]): (json.loads(row["display"]) if isinstance(row["display"], (str, bytes)) else row["display"])
        for _, row in df.iterrows()
        if row["display"] is not None
    }


def _update_single_chart(
    chart: gm.Chart, indicator_mapping: dict[int, int], api: AdminAPI, user_id: int | None = None
) -> int:
    """Update a single chart and return its ID."""
    # Update chart config
    config_new = update_chart_config(
        chart.config,
        indicator_mapping,
        get_schema_from_url(chart.config["$schema"]),
        indicator_config=_fetch_single_indicator_config(chart.config, indicator_mapping),
        dimension_display_baselines=_fetch_dimension_display_baselines(chart.config, indicator_mapping),
        # chart.config is chart_configs.full (the fully resolved config, inherited values
        # included) -- pass the real stored patch too so inheritance pruning can tell
        # "genuinely explicit" apart from "just showing through via inheritance from the
        # old indicator" (see ChartIndicatorUpdater.run's original_patch docstring).
        original_patch=chart.chart_config.patch,
    )

    # Get chart ID
    if chart.id:
        chart_id = chart.id
    elif "id" in chart.config:
        chart_id = chart.config["id"]
    else:
        raise ValueError(f"Chart {chart} does not have an ID in config.")

    # Push new chart to DB
    api.update_chart(chart_id=chart_id, chart_config=config_new, user_id=user_id)
    return chart_id


def get_affected_narrative_charts_cli(charts: list[gm.Chart]) -> list[gm.NarrativeChart]:
    """Get affected narrative charts for CLI (without Streamlit dependencies).

    Finds narrative charts by looking up which ones have the affected charts as parents.
    """
    log.info("Finding affected narrative charts...")
    parent_chart_ids = {chart.id for chart in charts if chart.id}
    with Session(get_engine()) as session:
        narrative_charts = gm.NarrativeChart.load_narrative_charts_by_parent_chart_ids(session, parent_chart_ids)
    log.info(f"Found {len(narrative_charts)} affected narrative charts")
    return narrative_charts


# User-facing text fields (FAUST) checked for stale overrides in narrative charts, as config paths.
FAUST_FIELDS = {
    "title": ("title",),
    "subtitle": ("subtitle",),
    "note": ("note",),
}
# Per-dimension display fields checked for stale overrides (under dimensions[].display).
FAUST_DISPLAY_TEXT_FIELDS = {
    "display.name": "name",
    "unit": "unit",
    "short unit": "shortUnit",
}
# Numeric display fields — no similarity notion; any pinned difference is reported.
FAUST_DISPLAY_NUMERIC_FIELDS = {
    "tolerance": "tolerance",
    "decimal places": "numDecimalPlaces",
    "significant figures": "numSignificantFigures",
}
# Similarity (difflib ratio) above which a differing override is treated as a stale copy of the
# parent's text rather than an intentional rewrite.
FAUST_STALE_SIMILARITY = 0.8


def _get_nested(config: dict, path: tuple[str, ...]):
    value = config
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def _normalize_chart_text(text: str) -> str:
    """Normalize chart text for staleness comparison: strip markdown links (keep anchor text) and
    collapse whitespace, so e.g. a parent note that gained detail-on-demand links still matches a
    narrative override that froze the older, link-less wording."""
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    return " ".join(text.split())


def _texts_look_stale(narrative_value: str, parent_value: str) -> bool:
    """True when two differing texts are similar enough that the narrative one looks like a stale
    copy of the parent's (rather than an intentional rewrite)."""
    narrative_norm = _normalize_chart_text(narrative_value)
    parent_norm = _normalize_chart_text(parent_value)
    return (
        narrative_norm == parent_norm
        or difflib.SequenceMatcher(None, narrative_norm, parent_norm).ratio() >= FAUST_STALE_SIMILARITY
    )


def _find_stale_faust_overrides(
    patch_config: dict, parent_config: dict, indicator_mapping: dict[int, int] | None = None
) -> list[tuple[str, str, str]]:
    """Find user-facing text overrides in a narrative chart that look like stale copies of the parent.

    A narrative chart pins in its patch any field it overrides. Overriding the title or subtitle is
    usually an intentional rewrite (that's what narrative charts are for), but an override that is
    *nearly identical* to the parent's current text is the signature of a stale copy: the narrative
    chart froze the parent's text at creation time and the parent has since evolved (e.g. gained
    detail-on-demand links in its note). Texts are compared after stripping markdown link syntax;
    equal-after-normalization or highly similar values are reported as stale. Checked fields:
    title, subtitle, note, and — per dimension, matched to the parent by (mapped) variable id —
    display.name, unit, shortUnit, and the numeric fields tolerance, numDecimalPlaces, and
    numSignificantFigures (numeric: any pinned difference is reported).
    Returns (field_label, narrative_value, parent_value) for each such field; identical raw values
    and substantially different text overrides are not reported.
    """
    stale = []

    # Top-level text fields.
    for label, path in FAUST_FIELDS.items():
        narrative_value = _get_nested(patch_config, path)
        parent_value = _get_nested(parent_config, path)
        if not isinstance(narrative_value, str) or not isinstance(parent_value, str):
            continue
        if narrative_value != parent_value and _texts_look_stale(narrative_value, parent_value):
            stale.append((label, narrative_value, parent_value))

    # Per-dimension display fields, matched to the parent dimension by variable id (translating
    # the narrative chart's pinned id through the mapping, since the patch may predate the remap).
    parent_displays = {dim.get("variableId"): dim.get("display") or {} for dim in parent_config.get("dimensions", [])}
    for dim in patch_config.get("dimensions", []):
        variable_id = dim.get("variableId")
        if indicator_mapping and variable_id in indicator_mapping:
            variable_id = indicator_mapping[variable_id]
        parent_display = parent_displays.get(variable_id)
        if parent_display is None:
            continue
        display = dim.get("display") or {}
        for label, key in FAUST_DISPLAY_TEXT_FIELDS.items():
            narrative_value = display.get(key)
            parent_value = parent_display.get(key)
            if not isinstance(narrative_value, str) or not isinstance(parent_value, str):
                continue
            if narrative_value != parent_value and _texts_look_stale(narrative_value, parent_value):
                stale.append((f"{label} (indicator {variable_id})", narrative_value, parent_value))
        # Numeric fields (tolerance, decimal places, significant figures) — no similarity notion;
        # any pinned difference is worth review.
        for label, key in FAUST_DISPLAY_NUMERIC_FIELDS.items():
            narrative_value = display.get(key)
            parent_value = parent_display.get(key)
            if narrative_value is not None and parent_value is not None and narrative_value != parent_value:
                stale.append((f"{label} (indicator {variable_id})", str(narrative_value), str(parent_value)))

    return stale


def _load_patches_and_parent_configs(
    narrative_charts: list[gm.NarrativeChart],
) -> tuple[dict[int, dict], dict[int, dict]]:
    """Load each narrative chart's config patch and each parent chart's full config."""

    def to_dict(value) -> dict:
        if isinstance(value, dict):
            return value
        if isinstance(value, (str, bytes)):
            return json.loads(value)
        return {}

    nc_ids = sorted({nc.id for nc in narrative_charts if nc.id})
    parent_ids = sorted({nc.parentChartId for nc in narrative_charts if nc.parentChartId})
    patches: dict[int, dict] = {}
    parent_configs: dict[int, dict] = {}
    if nc_ids:
        df = read_sql(
            "SELECT nc.id, cc.patch FROM narrative_charts nc JOIN chart_configs cc ON cc.id = nc.chartConfigId "
            f"WHERE nc.id IN ({','.join(str(int(i)) for i in nc_ids)})",
            engine=get_engine(),
        )
        patches = {int(row["id"]): to_dict(row["patch"]) for _, row in df.iterrows()}
    if parent_ids:
        df = read_sql(
            "SELECT c.id, cc.full FROM charts c JOIN chart_configs cc ON cc.id = c.configId "
            f"WHERE c.id IN ({','.join(str(int(i)) for i in parent_ids)})",
            engine=get_engine(),
        )
        parent_configs = {int(row["id"]): to_dict(row["full"]) for _, row in df.iterrows()}
    return patches, parent_configs


def _find_stale_lineage_variables(referenced_ids: set[int], indicator_mapping: dict[int, int]) -> dict[int, str]:
    """Find referenced indicators that belong to a different version of an upgraded dataset.

    `update_narrative_chart_config` only rewrites IDs present in `indicator_mapping`
    (old_version -> new_version). A narrative chart can still pin an ID from an even older
    version of the same dataset — left behind by a previous upgrade cycle — which the mapping
    cannot cover. Returns {variable_id: catalogPath} for such IDs so the caller can warn.
    """
    candidate_ids = referenced_ids - set(indicator_mapping) - set(indicator_mapping.values())
    if not candidate_ids:
        return {}

    def lineage(catalog_path: str) -> tuple[str, str] | None:
        # catalogPath format: grapher/<namespace>/<version>/<dataset>/<table>#<column>
        parts = catalog_path.split("/")
        if len(parts) < 4:
            return None
        return parts[1], parts[3]

    all_ids = candidate_ids | set(indicator_mapping.values())
    df = read_sql(
        f"SELECT id, catalogPath FROM variables WHERE id IN ({','.join(str(int(i)) for i in sorted(all_ids))})",
        engine=get_engine(),
    ).dropna(subset=["catalogPath"])
    paths = dict(zip(df["id"], df["catalogPath"]))

    upgraded_lineages = {lineage(paths[new_id]) for new_id in indicator_mapping.values() if new_id in paths} - {None}

    return {
        var_id: paths[var_id]
        for var_id in sorted(candidate_ids)
        if var_id in paths and lineage(paths[var_id]) in upgraded_lineages
    }


def push_new_narrative_charts_cli(
    narrative_charts: list[gm.NarrativeChart],
    indicator_mapping: dict[int, int],
    dry_run: bool = False,
) -> list[dict]:
    """Update narrative charts in the database (CLI version).

    Uses AdminAPI to:
    1. GET merged config (full config = parent + patch merged)
    2. Update variable IDs in the merged config
    3. PUT the updated merged config - backend recalculates the patch

    Returns a list of errors (each error is a dict with 'narrative_chart_id', 'name', and 'error' keys).
    """
    if not narrative_charts:
        log.warning("No narrative charts to update")
        return []

    if dry_run:
        log.info(
            f"DRY RUN: Would update {len(narrative_charts)} narrative charts with indicator mapping: {indicator_mapping}"
        )
        for nc in narrative_charts:
            log.info(f"DRY RUN: Would update narrative chart {nc.id} - {nc.name}")
        return []

    log.info(f"Updating {len(narrative_charts)} narrative charts...")

    user_id = get_grapher_user().id

    # API to interact with the admin tool
    api = AdminAPI(OWID_ENV)

    # Load patches and parent configs to check for stale user-facing text overrides (FAUST).
    patches, parent_configs = _load_patches_and_parent_configs(narrative_charts)

    # Update narrative charts sequentially
    successful_updates = 0
    skipped = 0
    errors: list[dict] = []

    for nc in narrative_charts:
        try:
            # Warn when the narrative chart pins user-facing text that is nearly identical to the
            # parent's current text — likely frozen at creation time and stale since.
            stale_faust = _find_stale_faust_overrides(
                patches.get(nc.id, {}), parent_configs.get(nc.parentChartId, {}), indicator_mapping
            )
            if stale_faust:
                details = "; ".join(
                    f"{field}: narrative={narrative_value!r} vs parent={parent_value!r}"
                    for field, narrative_value, parent_value in stale_faust
                )
                log.warning(
                    f"Narrative chart {nc.id} ({nc.name}) overrides user-facing text that is nearly "
                    f"identical to its parent chart {nc.parentChartId} — likely a stale copy of older "
                    f"parent text rather than an intentional rewrite: {details}. Setting the field to "
                    "the parent's exact text restores inheritance (identical values drop out of the patch)."
                )

            # Get full config via API (full config = parent + patch merged)
            response = api.get_narrative_chart(nc.id)
            full_config = response["configFull"]

            # Update variable IDs in the full config
            config_new = update_narrative_chart_config(full_config, indicator_mapping)

            # Check on every chart, not only unchanged ones: a config can mix a mappable indicator
            # with a stale pin from an even older dataset version, so a partial remap would
            # otherwise PUT the stale ID back silently.
            stale = _find_stale_lineage_variables(
                collect_variable_ids_from_narrative_config(full_config), indicator_mapping
            )
            if stale:
                log.warning(
                    f"Narrative chart {nc.id} ({nc.name}) pins indicators from a version of the upgraded "
                    f"dataset that the mapping does not cover: {stale}. These were likely left behind by a "
                    "previous upgrade cycle — remap them with an explicit mapping "
                    "(WizardDB.add_variable_mapping + push_new_narrative_charts_cli)."
                )

            if config_new == full_config:
                # Nothing in this config matched the mapping. PUTting an identical config would only
                # bump updatedAt and make the log claim a remap that never happened.
                if not stale:
                    log.info(f"Narrative chart {nc.id} ({nc.name}) references no mapped indicators — left unchanged.")
                skipped += 1
                continue

            # PUT the updated full config - backend will recalculate the patch
            api.update_narrative_chart(narrative_chart_id=nc.id, config=config_new, user_id=user_id)
            successful_updates += 1
            log.info(f"Successfully updated narrative chart {nc.id}")
        except AdminAPIError as e:
            log.error(f"Failed to update narrative chart {nc.id} ({nc.name}): {e}")
            errors.append(
                {
                    "narrative_chart_id": nc.id,
                    "name": nc.name,
                    "error": str(e),
                }
            )

    unchanged_note = f" ({skipped} left unchanged)" if skipped else ""
    if errors:
        log.warning(
            f"Updated {successful_updates}/{len(narrative_charts)} narrative charts with {len(errors)} failures"
            + unchanged_note
        )
    else:
        log.info(f"Updated {successful_updates}/{len(narrative_charts)} narrative charts{unchanged_note}")

    return errors


def push_new_charts_cli(
    charts: list[gm.Chart],
    indicator_mapping: dict[int, int],
    dry_run: bool = False,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> list[dict]:
    """Update charts in the database (CLI version).

    Returns a list of errors (each error is a dict with 'chart_id', 'chart_slug', and 'error' keys).
    """
    if not charts:
        log.warning("No charts to update")
        return []

    if dry_run:
        log.info(f"DRY RUN: Would update {len(charts)} charts with indicator mapping: {indicator_mapping}")
        for chart in charts:
            chart_url = OWID_ENV.chart_site(chart.slug) if chart.slug else f"Chart {chart.id}"
            log.info(f"DRY RUN: Would update chart {chart.id} - {chart_url}")
        return []

    log.info(f"Updating {len(charts)} charts in parallel (max_workers={max_workers})...")

    user_id = get_grapher_user().id

    # API to interact with the admin tool
    api = AdminAPI(OWID_ENV)

    # Update charts in parallel
    successful_updates = 0
    errors: list[dict] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all chart updates
        future_to_chart = {
            executor.submit(_update_single_chart, chart, indicator_mapping, api, user_id): chart for chart in charts
        }

        # Process completed updates - collect AdminAPIError but continue processing
        for future in as_completed(future_to_chart):
            chart = future_to_chart[future]
            try:
                chart_id = future.result()
                successful_updates += 1
                log.info(f"Successfully updated chart {chart_id}")
            except AdminAPIError as e:
                chart_id = chart.id or chart.config.get("id", "unknown")
                chart_slug = chart.slug or chart.config.get("slug", "unknown")
                log.error(f"Failed to update chart {chart_id} ({chart_slug}): {e}")
                errors.append(
                    {
                        "chart_id": chart_id,
                        "chart_slug": chart_slug,
                        "error": str(e),
                    }
                )

    if errors:
        log.warning(f"Updated {successful_updates}/{len(charts)} charts with {len(errors)} failures")
    else:
        log.info(f"Successfully updated all {successful_updates} charts")

    return errors


def cli_upgrade_indicators(dry_run: bool = False, max_workers: int = DEFAULT_MAX_WORKERS) -> dict:
    """Main CLI function to upgrade indicators using existing variable mapping in DB.

    Returns a dictionary with:
        - 'success': bool indicating if all updates succeeded
        - 'chart_errors': list of chart error dicts
        - 'narrative_chart_errors': list of narrative chart error dicts
    """
    log.info("Starting indicator upgrade from existing variable mapping in database")

    # 1. Load variable mapping from database
    indicator_mapping = WizardDB.get_variable_mapping()

    if not indicator_mapping:
        log.error("No variable mappings found in database. Cannot proceed.")
        log.error("Use the Streamlit UI to create a variable mapping first, or manually add one to the database.")
        return {"success": False, "chart_errors": [], "narrative_chart_errors": []}

    log.info(f"Found {len(indicator_mapping)} variable mappings:")
    log.info(f"{pd.DataFrame(list(indicator_mapping.items()), columns=['old_id', 'new_id'])}")

    # 2. Get affected charts
    charts = get_affected_charts_cli(indicator_mapping)

    if not charts:
        log.warning("No charts affected by this mapping")
        return {"success": True, "chart_errors": [], "narrative_chart_errors": []}

    # 3. Show affected charts
    log.info("Affected charts:")
    for chart in charts:
        chart_url = OWID_ENV.chart_site(chart.slug) if chart.slug else f"Chart {chart.id}"
        log.info(f"  - Chart {chart.id}: {chart_url}")

    # 4. Get affected narrative charts (children of affected charts)
    narrative_charts = get_affected_narrative_charts_cli(charts)

    if narrative_charts:
        log.info("Affected narrative charts:")
        for nc in narrative_charts:
            log.info(f"  - Narrative chart {nc.id}: {nc.name}")

    # 5. Update charts (collect errors instead of failing)
    chart_errors = push_new_charts_cli(charts, indicator_mapping, dry_run=dry_run, max_workers=max_workers)

    # 6. Update narrative charts (collect errors instead of failing)
    narrative_chart_errors = push_new_narrative_charts_cli(narrative_charts, indicator_mapping, dry_run=dry_run)

    # 7. Report final status
    if dry_run:
        log.info("DRY RUN completed - no changes made")
        return {"success": True, "chart_errors": [], "narrative_chart_errors": []}
    else:
        total_errors = len(chart_errors) + len(narrative_chart_errors)
        if total_errors > 0:
            log.error(f"Indicator upgrade completed with {total_errors} errors:")
            if chart_errors:
                log.error(f"  Chart errors ({len(chart_errors)}):")
                for err in chart_errors:
                    log.error(f"    - Chart {err['chart_id']} ({err['chart_slug']}): {err['error']}")
            if narrative_chart_errors:
                log.error(f"  Narrative chart errors ({len(narrative_chart_errors)}):")
                for err in narrative_chart_errors:
                    log.error(f"    - Narrative chart {err['narrative_chart_id']} ({err['name']}): {err['error']}")
            return {"success": False, "chart_errors": chart_errors, "narrative_chart_errors": narrative_chart_errors}
        else:
            log.info("Indicator upgrade completed successfully!")
            return {"success": True, "chart_errors": [], "narrative_chart_errors": []}


@click.command()
@click.option("--dry-run", is_flag=True, help="Preview changes without applying them")
@click.option(
    "--max-workers",
    default=DEFAULT_MAX_WORKERS,
    help=f"Maximum number of parallel workers (default: {DEFAULT_MAX_WORKERS})",
)
def main(dry_run: bool, max_workers: int):
    """CLI tool for upgrading chart indicators using existing variable mapping in database."""
    result = cli_upgrade_indicators(dry_run=dry_run, max_workers=max_workers)
    if not result["success"]:
        raise RuntimeError(
            f"Indicator upgrade completed with {len(result['chart_errors']) + len(result['narrative_chart_errors'])} errors. See logs above for details."
        )


if __name__ == "__main__":
    main()

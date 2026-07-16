"""Chart approval CLI.

This CLI provides tools for automatically approving chart diffs where configs are identical
between staging and production environments.
"""

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import unified_diff

import rich_click as click
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.chart_approval.config_utils import (
    blank_variable_ids,
    diff_chart_dimension_data,
    dimension_diff_within_tolerance,
    get_chart_config_with_hashes,
)
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiffsLoader
from etl.config import ENV_FILE_PROD, OWID_ENV, OWIDEnv

log = get_logger()


def approve_identical_chart_diffs(
    dry_run: bool = True,
    chart_ids: list[int] | None = None,
    verbose: bool = False,
    use_rounding: bool = True,
    use_max_year_hash: bool = False,
    show_data_diff: bool = False,
    allow_small_changes: bool = False,
    tolerance_pct: float = 1.0,
    tolerance_abs_floor: float = 1e-6,
    max_changed_points: int = 5,
    max_new_points: int = 1000,
):
    """Core function to approve chart diffs with identical configurations.

    Args:
        dry_run: If True, only shows what would be approved without making changes
        chart_ids: Optional list of specific chart IDs to check. If None, checks all pending charts.
        verbose: If True, shows detailed diff for charts with different configs
        use_rounding: If True, round numeric values to meaningful precision before comparing
        use_max_year_hash: If True, use only max year for hashing instead of full data
        show_data_diff: If True, for skipped charts fetch and print the actual data points that
            differ between environments per dimension (y/x/size/color), not just config hashes
        allow_small_changes: If True, also approve charts whose config is otherwise identical (same
            fields, same dimension slots) but whose dimension data differs only by small amounts —
            see tolerance_pct / tolerance_abs_floor / max_changed_points / max_new_points
        tolerance_pct: Max relative change (%) allowed per changed data point for allow_small_changes
        tolerance_abs_floor: Absolute floor for the tolerance check, to avoid huge relative % near zero
        max_changed_points: Max number of changed points per dimension allowed for allow_small_changes;
            charts with more changed points are still sent to manual review
        max_new_points: Max number of newly-added points (present in staging only, e.g. a new year of
            coverage) per dimension allowed for allow_small_changes; charts with more are still sent to
            manual review, since an unexpectedly large coverage jump can visibly change the chart

    Returns:
        Tuple of (approved_count, checked_count)
    """
    log.info("Starting chart approval process")

    # Initialize environments
    assert ENV_FILE_PROD is not None, "ENV_FILE_PROD must be set"
    PROD_ENV = OWIDEnv.from_env_file(ENV_FILE_PROD)

    # Initialize chart diff loader
    chart_diff_loader = ChartDiffsLoader(OWID_ENV.engine, PROD_ENV.engine)

    # Get summary of all chart diffs (config changes only)
    log.info("Fetching chart diffs summary")
    df = chart_diff_loader.get_diffs_summary_df(
        config=True,
        metadata=False,
        data=False,
        skip_analytics=True,
    )

    if df.empty:
        log.info("No chart diffs found")
        return 0, 0

    # Filter to only pending charts (not approved, not rejected)
    pending_charts = df[(~df.is_approved) & (~df.is_rejected)].copy()

    if pending_charts.empty:
        log.info("No pending chart diffs found")
        return 0, 0

    # Filter by specific chart IDs if provided
    if chart_ids is not None:
        pending_charts = pending_charts[pending_charts.chart_id.isin(chart_ids)]
        if pending_charts.empty:
            log.info(f"No pending chart diffs found for specified chart IDs: {chart_ids}")
            return 0, 0

    log.info(f"Found {len(pending_charts)} pending chart diffs")

    # Fetch configs in parallel using threads
    chart_ids_to_check = pending_charts.chart_id.tolist()
    configs_staging = {}
    configs_prod = {}

    def fetch_config(chart_id: int, env: OWIDEnv) -> tuple[int, dict]:
        """Fetch config for a single chart"""
        return chart_id, get_chart_config_with_hashes(
            chart_id, env, round_values=use_rounding, use_max_year_hash=use_max_year_hash
        )

    log.info("Fetching configs from staging environment")
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_config, chart_id, OWID_ENV): chart_id for chart_id in chart_ids_to_check}
        for future in as_completed(futures):
            chart_id, config = future.result()
            configs_staging[chart_id] = config

    log.info("Fetching configs from production environment")
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_config, chart_id, PROD_ENV): chart_id for chart_id in chart_ids_to_check}
        for future in as_completed(futures):
            chart_id, config = future.result()
            configs_prod[chart_id] = config

    # Check each chart for identical configs and collect ones to approve
    approved_count = 0
    checked_count = 0
    charts_to_approve = []

    # First pass (cheap, in-memory): classify charts and figure out which differing charts need their
    # dimension data fetched (for --allow-small-changes and/or --show-data-diff).
    exact_match_chart_ids = []
    differing_chart_ids = []
    has_map_time_by_chart = {}
    needs_tolerance_check = {}

    for chart_id in chart_ids_to_check:
        checked_count += 1

        config_staging = configs_staging.get(chart_id)
        config_prod = configs_prod.get(chart_id)

        if config_staging is None or config_prod is None:
            log.warning("⚠️ Config not found for chart", chart_id=chart_id)
            continue

        # Check if chart has map.time set AND actually displays a map tab (which should be reviewed manually)
        # Note: map.time only matters if the chart shows a map tab (hasMapTab: true or tab: "map")
        has_map_time = False
        if "map" in config_staging and isinstance(config_staging["map"], dict):
            if "time" in config_staging["map"]:
                # Only flag if the chart actually shows a map tab
                has_map_tab = config_staging.get("hasMapTab", False)
                default_tab_is_map = config_staging.get("tab") == "map"
                if has_map_tab or default_tab_is_map:
                    has_map_time = True
        has_map_time_by_chart[chart_id] = has_map_time

        if config_staging == config_prod:
            exact_match_chart_ids.append(chart_id)
        else:
            differing_chart_ids.append(chart_id)
            # Only worth fetching dimension data for tolerance checking if the rest of the config
            # (everything except which variable IDs are referenced) is otherwise identical.
            needs_tolerance_check[chart_id] = allow_small_changes and blank_variable_ids(
                config_staging
            ) == blank_variable_ids(config_prod)

    for chart_id in exact_match_chart_ids:
        has_map_time = has_map_time_by_chart[chart_id]
        if has_map_time:
            log.warning("⚠️ Chart has map.time set - requires manual review", chart_id=chart_id)
        elif dry_run:
            log.info("✅ Would approve chart (dry run)", chart_id=chart_id)
            approved_count += 1
        else:
            charts_to_approve.append(chart_id)

    # Second pass: fetch dimension data in parallel for every differing chart that needs it, either to
    # check tolerance or (with --show-data-diff) to print the actual value changes.
    dimension_diffs_by_chart: dict[int, list] = {}
    chart_ids_needing_dimension_diffs = [
        chart_id for chart_id in differing_chart_ids if needs_tolerance_check[chart_id] or show_data_diff
    ]

    def fetch_dimension_diffs(chart_id: int):
        return chart_id, diff_chart_dimension_data(
            chart_id, OWID_ENV.engine, OWID_ENV, PROD_ENV.engine, PROD_ENV, round_values=use_rounding
        )

    if chart_ids_needing_dimension_diffs:
        log.info(f"Fetching dimension data for {len(chart_ids_needing_dimension_diffs)} differing charts")
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {
                executor.submit(fetch_dimension_diffs, chart_id): chart_id
                for chart_id in chart_ids_needing_dimension_diffs
            }
            for future in as_completed(futures):
                chart_id, dimension_diffs = future.result()
                dimension_diffs_by_chart[chart_id] = dimension_diffs

    for chart_id in differing_chart_ids:
        config_staging = configs_staging[chart_id]
        config_prod = configs_prod[chart_id]
        has_map_time = has_map_time_by_chart[chart_id]

        dimension_diffs = dimension_diffs_by_chart.get(chart_id)
        approved_via_tolerance = (
            needs_tolerance_check[chart_id]
            and dimension_diffs is not None
            and all(
                dimension_diff_within_tolerance(
                    d, tolerance_pct, tolerance_abs_floor, max_changed_points, max_new_points
                )
                for d in dimension_diffs
            )
        )

        if approved_via_tolerance:
            if has_map_time:
                log.warning("⚠️ Chart has map.time set - requires manual review", chart_id=chart_id)
            elif dry_run:
                log.info("🟡 Would approve chart (dry run, small changes within tolerance)", chart_id=chart_id)
                approved_count += 1
            else:
                charts_to_approve.append(chart_id)
            continue

        log.info("⏭️ Configs differ - skipping", chart_id=chart_id)
        if verbose:
            prod_json = json.dumps(config_prod, indent=2, sort_keys=True).splitlines(keepends=True)
            staging_json = json.dumps(config_staging, indent=2, sort_keys=True).splitlines(keepends=True)

            diff_lines = list(
                unified_diff(
                    prod_json,
                    staging_json,
                    fromfile=f"production (chart {chart_id})",
                    tofile=f"staging (chart {chart_id})",
                    lineterm="",
                )
            )

            if diff_lines:
                print(f"\n{'=' * 80}")
                print(f"Config differences for chart {chart_id}:")
                print(f"{'=' * 80}")
                print("".join(diff_lines))
                print(f"{'=' * 80}\n")

        if show_data_diff:
            if dimension_diffs is None:
                dimension_diffs = diff_chart_dimension_data(
                    chart_id, OWID_ENV.engine, OWID_ENV, PROD_ENV.engine, PROD_ENV, round_values=use_rounding
                )
            if not dimension_diffs:
                print(f"\nChart {chart_id}: config differs but no dimension's underlying data changed.\n")
            for dim_diff in dimension_diffs:
                print(f"\n{'-' * 80}")
                print(
                    f"Chart {chart_id}, dimension '{dim_diff['property']}' "
                    f"(staging variable {dim_diff['variable_id_a']} vs prod variable {dim_diff['variable_id_b']}):"
                )
                print(
                    f"  {dim_diff['n_changed']} of {dim_diff['n_common']} common points changed, "
                    f"{dim_diff['n_only_a']} only in staging, {dim_diff['n_only_b']} only in prod"
                )
                for entity, year, value_a, value_b in dim_diff["examples_changed"]:
                    print(f"    {entity} {year}: {value_b} -> {value_a}")
                if dim_diff["examples_only_a"]:
                    print(f"    Only in staging: {', '.join(dim_diff['examples_only_a'])}")
                if dim_diff["examples_only_b"]:
                    print(f"    Only in prod: {', '.join(dim_diff['examples_only_b'])}")
                print(f"{'-' * 80}\n")

    # Batch approve all charts at once
    if charts_to_approve:
        log.info(f"Approving {len(charts_to_approve)} charts in batch")
        diffs = chart_diff_loader.get_diffs(chart_ids=charts_to_approve, sync=True, skip_analytics=True)
        with Session(OWID_ENV.engine) as session:
            for diff in diffs:
                diff.approve(session)
                log.info("✅ Chart approved", chart_id=diff.chart_id)
                approved_count += 1

    if dry_run:
        log.info(f"DRY RUN completed: {approved_count} charts would be approved out of {checked_count} checked")
    else:
        log.info(f"Chart approval completed: {approved_count} charts approved out of {checked_count} checked")

    return approved_count, checked_count


@click.command(name="approve")
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Preview which charts would be approved without actually approving them.",
)
@click.option(
    "--chart-id",
    multiple=True,
    type=int,
    help="Specific chart ID(s) to check. Can be specified multiple times. If not provided, checks all pending charts.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show detailed config differences for charts that differ between environments.",
)
@click.option(
    "--no-rounding",
    is_flag=True,
    default=False,
    help="Disable intelligent rounding before comparing data (require exact match).",
)
@click.option(
    "--use-max-year-hash",
    is_flag=True,
    default=False,
    help="Use only max year from each indicator for comparison (ignores data values).",
)
@click.option(
    "--show-data-diff",
    is_flag=True,
    default=False,
    help="For skipped charts, fetch and print the actual data points that differ per dimension "
    "(y/x/size/color) between staging and production, instead of just a config hash mismatch. "
    "Slower (extra API calls per skipped chart) — combine with --chart-id for a single chart.",
)
@click.option(
    "--allow-small-changes",
    is_flag=True,
    default=False,
    help="Also approve charts whose config is otherwise identical but whose dimension data changed "
    "only by small amounts (e.g. minor source revisions to a handful of country-years). Controlled by "
    "--tolerance-pct / --tolerance-abs-floor / --max-changed-points. Slower — fetches full data per "
    "dimension for every chart whose config differs.",
)
@click.option(
    "--tolerance-pct",
    type=float,
    default=1.0,
    help="With --allow-small-changes: max relative change (%) allowed per changed data point.",
)
@click.option(
    "--tolerance-abs-floor",
    type=float,
    default=1e-6,
    help="With --allow-small-changes: absolute floor for the tolerance check, so tiny values near zero "
    "don't produce a misleadingly huge relative change.",
)
@click.option(
    "--max-changed-points",
    type=int,
    default=5,
    help="With --allow-small-changes: max number of changed points per dimension allowed before the "
    "chart is still sent to manual review.",
)
@click.option(
    "--max-new-points",
    type=int,
    default=1000,
    help="With --allow-small-changes: max number of newly-added points (present in staging only, e.g. "
    "a new year of coverage) per dimension allowed before the chart is still sent to manual review — "
    "an unexpectedly large coverage jump can visibly change the chart even with no other value changes.",
)
def cli(
    dry_run: bool,
    chart_id: tuple[int, ...],
    verbose: bool,
    no_rounding: bool,
    use_max_year_hash: bool,
    show_data_diff: bool,
    allow_small_changes: bool,
    tolerance_pct: float,
    tolerance_abs_floor: float,
    max_changed_points: int,
    max_new_points: int,
) -> None:
    """Automatically approve chart diffs with identical data. This is done by taking their configs and replacing variable IDs with hashes of their data.

    If the configs are then identical, the chart is approved.

    The comparison process:
    1. Fetches all pending chart diffs (not yet approved/rejected)
    2. For each chart, compares the normalized config between environments
    3. Approves charts where configs are identical
    4. Reports results
    """
    chart_ids = list(chart_id) if chart_id else None
    approve_identical_chart_diffs(
        dry_run=dry_run,
        chart_ids=chart_ids,
        verbose=verbose,
        use_rounding=not no_rounding,
        use_max_year_hash=use_max_year_hash,
        show_data_diff=show_data_diff,
        allow_small_changes=allow_small_changes,
        tolerance_pct=tolerance_pct,
        tolerance_abs_floor=tolerance_abs_floor,
        max_changed_points=max_changed_points,
        max_new_points=max_new_points,
    )


if __name__ == "__main__":
    cli()

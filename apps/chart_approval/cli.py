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

from apps.chart_approval.config_utils import get_chart_config_with_hashes
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiffsLoader
from etl.config import ENV_FILE_PROD, OWID_ENV, OWIDEnv

log = get_logger()


def approve_identical_chart_diffs(dry_run: bool = True, chart_ids: list[int] | None = None, verbose: bool = False):
    """Core function to approve chart diffs with identical configurations.

    Args:
        dry_run: If True, only shows what would be approved without making changes
        chart_ids: Optional list of specific chart IDs to check. If None, checks all pending charts.
        verbose: If True, shows detailed diff for charts with different configs

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
        return chart_id, get_chart_config_with_hashes(chart_id, env)

    log.info("Fetching configs from staging environment")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_config, chart_id, OWID_ENV): chart_id for chart_id in chart_ids_to_check}
        for future in as_completed(futures):
            chart_id, config = future.result()
            configs_staging[chart_id] = config

    log.info("Fetching configs from production environment")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_config, chart_id, PROD_ENV): chart_id for chart_id in chart_ids_to_check}
        for future in as_completed(futures):
            chart_id, config = future.result()
            configs_prod[chart_id] = config

    # Check each chart for identical configs and approve immediately
    approved_count = 0
    checked_count = 0

    for chart_id in chart_ids_to_check:
        checked_count += 1

        config_staging = configs_staging.get(chart_id)
        config_prod = configs_prod.get(chart_id)

        if config_staging is None or config_prod is None:
            log.warning("⚠️ Config not found for chart", chart_id=chart_id)
            continue

        # Check if chart has map.time set (which should be reviewed manually)
        has_map_time = False
        if "map" in config_staging and isinstance(config_staging["map"], dict):
            if "time" in config_staging["map"]:
                has_map_time = True

        # Compare configs
        if config_staging == config_prod:
            if has_map_time:
                log.warning("⚠️ Chart has map.time set - requires manual review", chart_id=chart_id)
            elif dry_run:
                log.info("✅ Would approve chart (dry run)", chart_id=chart_id)
                approved_count += 1
            else:
                # Get chart diff and approve immediately
                diffs = chart_diff_loader.get_diffs(chart_ids=[chart_id], sync=True)
                if diffs:
                    with Session(OWID_ENV.engine) as session:
                        diffs[0].approve(session)
                        log.info("✅ Chart approved", chart_id=chart_id)
                        approved_count += 1
                else:
                    log.warning("⚠️ No diff found for chart", chart_id=chart_id)
        else:
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
                    print(f"\n{'='*80}")
                    print(f"Config differences for chart {chart_id}:")
                    print(f"{'='*80}")
                    print("".join(diff_lines))
                    print(f"{'='*80}\n")

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
def cli(
    dry_run: bool,
    chart_id: tuple[int, ...],
    verbose: bool,
) -> None:
    """Automatically approve chart diffs with identical data. This is done by
    taking their configs and replacing variable IDs with hashes of their data.
    If the configs are then identical, the chart is approved.

    The comparison process:
    1. Fetches all pending chart diffs (not yet approved/rejected)
    2. For each chart, compares the normalized config between environments
    3. Approves charts where configs are identical
    4. Reports results
    """
    chart_ids = list(chart_id) if chart_id else None
    approve_identical_chart_diffs(dry_run=dry_run, chart_ids=chart_ids, verbose=verbose)


if __name__ == "__main__":
    cli()

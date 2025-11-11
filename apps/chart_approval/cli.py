"""Chart approval CLI.

This CLI provides tools for automatically approving chart diffs where configs are identical
between staging and production environments.
"""

import rich_click as click
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.chart_approval.config_utils import get_chart_config_with_hashes
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiffsLoader
from etl.config import ENV_FILE_PROD, OWID_ENV, OWIDEnv

log = get_logger()


def approve_identical_chart_diffs(dry_run: bool = True):
    """Core function to approve chart diffs with identical configurations.

    Args:
        dry_run: If True, only shows what would be approved without making changes

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

    log.info(f"Found {len(pending_charts)} pending chart diffs")

    # Check each chart for identical configs and approve immediately
    approved_count = 0
    checked_count = 0

    for chart_id in pending_charts.chart_id:
        checked_count += 1

        # Get normalized configs from both environments
        config_staging = get_chart_config_with_hashes(chart_id, OWID_ENV)
        config_prod = get_chart_config_with_hashes(chart_id, PROD_ENV)

        # Compare configs
        if config_staging == config_prod:
            if dry_run:
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
def cli(
    dry_run: bool,
) -> None:
    """Automatically approve chart diffs with identical data. This is done by taking their configs and replacing variable IDs with hashes of their data.

    If the configs are then identical, the chart is approved.

    The comparison process:
    1. Fetches all pending chart diffs (not yet approved/rejected)
    2. For each chart, compares the normalized config between environments
    3. Approves charts where configs are identical
    4. Reports results
    """
    approve_identical_chart_diffs(dry_run=dry_run)


if __name__ == "__main__":
    cli()

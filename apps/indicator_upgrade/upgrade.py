"""CLI functions for upgrading indicators and charts."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

import click
import pandas as pd
from structlog import get_logger

import etl.grapher.model as gm
from apps.chart_sync.admin_api import AdminAPI
from apps.wizard.utils.cached import get_grapher_user
from apps.wizard.utils.db import WizardDB
from etl.config import OWID_ENV
from etl.files import get_schema_from_url
from etl.indicator_upgrade.indicator_update import find_charts_from_variable_ids, update_chart_config

# Default number of parallel workers
DEFAULT_MAX_WORKERS = 3

log = get_logger()


def get_affected_charts_cli(indicator_mapping: Dict[int, int]) -> List[gm.Chart]:
    """Get affected charts for CLI (without Streamlit dependencies)."""
    log.info("Finding affected charts...")
    charts = find_charts_from_variable_ids(set(indicator_mapping.keys()))
    log.info(f"Found {len(charts)} affected charts")
    return charts


def _update_single_chart(chart: gm.Chart, indicator_mapping: Dict[int, int], api: AdminAPI) -> int:
    """Update a single chart and return its ID."""
    # Update chart config
    config_new = update_chart_config(
        chart.config,
        indicator_mapping,
        get_schema_from_url(chart.config["$schema"]),
    )

    # Get chart ID
    if chart.id:
        chart_id = chart.id
    elif "id" in chart.config:
        chart_id = chart.config["id"]
    else:
        raise ValueError(f"Chart {chart} does not have an ID in config.")

    # Push new chart to DB
    api.update_chart(chart_id=chart_id, chart_config=config_new)
    return chart_id


def push_new_charts_cli(
    charts: List[gm.Chart],
    indicator_mapping: Dict[int, int],
    dry_run: bool = False,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> None:
    """Update charts in the database (CLI version)."""
    if not charts:
        log.warning("No charts to update")
        return

    if dry_run:
        log.info(f"DRY RUN: Would update {len(charts)} charts with indicator mapping: {indicator_mapping}")
        for chart in charts:
            chart_url = OWID_ENV.chart_site(chart.slug) if chart.slug else f"Chart {chart.id}"
            log.info(f"DRY RUN: Would update chart {chart.id} - {chart_url}")
        return

    log.info(f"Updating {len(charts)} charts in parallel (max_workers={max_workers})...")

    grapher_user_id = get_grapher_user().id

    # API to interact with the admin tool
    api = AdminAPI(OWID_ENV, grapher_user_id=grapher_user_id)

    # Update charts in parallel
    successful_updates = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all chart updates
        future_to_chart = {
            executor.submit(_update_single_chart, chart, indicator_mapping, api): chart for chart in charts
        }

        # Process completed updates - fail fast on any error
        for future in as_completed(future_to_chart):
            chart_id = future.result()  # This will raise if the future failed
            successful_updates += 1
            log.info(f"Successfully updated chart {chart_id}")

    log.info(f"Successfully updated all {successful_updates} charts")


def cli_upgrade_indicators(dry_run: bool = False, max_workers: int = DEFAULT_MAX_WORKERS) -> None:
    """Main CLI function to upgrade indicators using existing variable mapping in DB."""
    log.info("Starting indicator upgrade from existing variable mapping in database")

    # 1. Load variable mapping from database
    indicator_mapping = WizardDB.get_variable_mapping()

    if not indicator_mapping:
        log.error("No variable mappings found in database. Cannot proceed.")
        log.error("Use the Streamlit UI to create a variable mapping first, or manually add one to the database.")
        return

    log.info(f"Found {len(indicator_mapping)} variable mappings:")
    log.info(f"{pd.DataFrame(list(indicator_mapping.items()), columns=['old_id', 'new_id'])}")

    # 2. Get affected charts
    charts = get_affected_charts_cli(indicator_mapping)

    if not charts:
        log.warning("No charts affected by this mapping")
        return

    # 3. Show affected charts
    log.info("Affected charts:")
    for chart in charts:
        chart_url = OWID_ENV.chart_site(chart.slug) if chart.slug else f"Chart {chart.id}"
        log.info(f"  - Chart {chart.id}: {chart_url}")

    # 4. Update charts
    push_new_charts_cli(charts, indicator_mapping, dry_run=dry_run, max_workers=max_workers)

    if not dry_run:
        log.info("Indicator upgrade completed successfully!")
    else:
        log.info("DRY RUN completed - no changes made")


@click.command()
@click.option("--dry-run", is_flag=True, help="Preview changes without applying them")
@click.option(
    "--max-workers",
    default=DEFAULT_MAX_WORKERS,
    help=f"Maximum number of parallel workers (default: {DEFAULT_MAX_WORKERS})",
)
def main(dry_run: bool, max_workers: int):
    """CLI tool for upgrading chart indicators using existing variable mapping in database."""
    cli_upgrade_indicators(dry_run=dry_run, max_workers=max_workers)


if __name__ == "__main__":
    main()

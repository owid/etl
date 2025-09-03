"""Indicator upgrader CLI.

This CLI provides tools for managing indicator upgrades, including:
- Matching variables between old and new datasets
- Upgrading indicators in the database
- Undoing indicator upgrades
"""

import rich_click as click
from rich_click.rich_group import RichGroup
from structlog import get_logger

from apps.indicator_upgrade.match import main as match_main
from apps.indicator_upgrade.upgrade import (
    cli_upgrade_indicators,
    get_affected_charts_cli,
    push_new_charts_cli,
)
from apps.wizard.utils.db import WizardDB

log = get_logger()


@click.group(name="indicator-upgrade", cls=RichGroup, help=__doc__)
def cli() -> None:
    """Manage indicator upgrades between dataset versions."""
    pass


@cli.command(name="match")
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Print the mappings without applying them.",
)
@click.option(
    "-old",
    "--old-dataset-id",
    type=int,
    help="Old dataset ID (as defined in grapher).",
    required=True,
)
@click.option(
    "-new",
    "--new-dataset-id",
    type=int,
    help="New dataset ID (as defined in grapher).",
    required=True,
)
@click.option(
    "-s",
    "--similarity-name",
    type=str,
    default="partial_ratio",
    help=(
        "Name of similarity function to use when fuzzy matching variables."
        " Default: partial_ratio. Available methods:"
        " token_set_ratio, token_sort_ratio, partial_ratio,"
        " partial_token_set_ratio, partial_token_sort_ratio, ratio,"
        " quick_ratio, weighted_ratio."
    ),
)
@click.option(
    "-a",
    "--add-identical-pairs",
    is_flag=True,
    default=False,
    help=(
        "If given, add variables with identical names in both datasets to the"
        " comparison. If not given, omit such variables and assume they should be"
        " paired."
    ),
)
@click.option(
    "-m",
    "--max-suggestions",
    type=int,
    default=10,
    help=(
        "Number of suggestions to show per old variable. That is, for every old"
        " variable at most [--max-suggestions] suggestions will be listed."
    ),
)
@click.option(
    "--no-interactive",
    is_flag=True,
    default=False,
    help=(
        "Skip interactive prompts and automatically map variables based on similarity threshold."
        " Best matches above the threshold will be selected automatically."
    ),
)
@click.option(
    "--auto-threshold",
    type=float,
    default=80.0,
    help="Similarity threshold (0-100) for automatic mapping when --no-interactive is used. Default: 80.0",
)
def match_command(
    old_dataset_id: int,
    new_dataset_id: int,
    dry_run: bool,
    add_identical_pairs: bool,
    similarity_name: str,
    max_suggestions: int,
    no_interactive: bool,
    auto_threshold: float,
) -> None:
    """Match variable IDs from an old dataset to a new dataset.

    After a dataset has been uploaded to OWID's MySQL database, we need to pair new variable IDs with the old ones,
    so that all graphs update properly.

    If the variable names are identical, the task is trivial: find indexes of old variables and map them to the indexes of the identical variables in the new dataset. However, if variable names have changed (or the number of variables have changed) the pairing may need to be done manually. This CLI helps in either scenario.
    """
    match_main(
        old_dataset_id=old_dataset_id,
        new_dataset_id=new_dataset_id,
        dry_run=dry_run,
        match_identical=not add_identical_pairs,
        similarity_name=similarity_name,
        max_suggestions=max_suggestions,
        no_interactive=no_interactive,
        auto_threshold=auto_threshold,
    )


@cli.command(name="upgrade")
@click.option("--dry-run", is_flag=True, help="Preview changes without applying them")
def upgrade_command(dry_run: bool) -> None:
    """Upgrade indicators to use new variable mappings.

    This command will apply the variable mappings stored in the database
    to update all charts and other references to use the new variable IDs.

    The variable mappings must have been previously created using the 'match' command
    or through the Streamlit UI.
    """
    cli_upgrade_indicators(dry_run=dry_run)


@cli.command(name="undo")
@click.option("--dry-run", is_flag=True, help="Preview changes without applying them")
def undo_command(dry_run: bool) -> None:
    """Undo the last indicator upgrade.

    This command will reverse the most recent variable mapping operation,
    restoring charts and other references to use the previous variable IDs.
    """
    log.info("Starting indicator upgrade undo")

    # 1. Load variable mapping from database
    indicator_mapping = WizardDB.get_variable_mapping()

    if not indicator_mapping:
        log.error("No variable mappings found in database. Cannot proceed.")
        log.error("There are no indicator upgrades to undo.")
        return

    log.info(f"Found {len(indicator_mapping)} variable mappings to undo")

    # 2. Invert the mapping (swap old and new IDs to reverse the upgrade)
    mapping_inverted = {v: k for k, v in indicator_mapping.items()}
    log.info(f"Inverted mapping: {mapping_inverted}")

    # 3. Get affected charts
    charts = get_affected_charts_cli(mapping_inverted)

    if not charts:
        log.warning("No charts affected by the inverted mapping")
    else:
        log.info(f"Found {len(charts)} charts that will be reverted")
        for chart in charts:
            from etl.config import OWID_ENV

            chart_url = OWID_ENV.chart_site(chart.slug) if chart.slug else f"Chart {chart.id}"
            log.info(f"  - Chart {chart.id}: {chart_url}")

        # 4. Update charts with inverted mapping (revert the changes)
        push_new_charts_cli(charts, mapping_inverted, dry_run=dry_run)

    # 5. Delete variable mapping from database (only if not dry run)
    if not dry_run:
        WizardDB.delete_variable_mapping()
        log.info("Deleted variable mapping from database")
    else:
        log.info("DRY RUN: Would delete variable mapping from database")

    if not dry_run:
        log.info("Indicator upgrade undo completed successfully!")
    else:
        log.info("DRY RUN completed - no changes made")


if __name__ == "__main__":
    cli()

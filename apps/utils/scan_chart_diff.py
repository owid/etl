import click
import streamlit as st
from rich_click.rich_command import RichCommand
from sqlalchemy.exc import OperationalError, ProgrammingError
from structlog import get_logger

from apps.owidbot import github_utils as gh_utils
from apps.owidbot.cli import cli as owidbot_cli
from etl import config

config.enable_sentry()

log = get_logger()


@click.command(name="scan-chart-diff", cls=RichCommand, help=__doc__)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    type=bool,
    help="Print to console, do not post to Github.",
)
def cli(dry_run: bool) -> None:
    """Scan all open PRs in the etl repository and run
    `etl owidbot etl/branch --services chart-diff` against them.
    """
    active_prs = gh_utils.get_prs_from_repo("etl")

    log.info("scan-chart-diff.init", active_prs=active_prs)

    for pr in active_prs:
        log.info("scan-chart-diff.start", pr=pr)
        args = [f"etl/{pr}", "--services", "chart-diff"]
        if dry_run:
            args.append("--dry-run")

        try:
            # Make sure to clear state, otherwise we'd be using cached state from previous
            # branch.
            st.session_state.clear()
            owidbot_cli(args, standalone_mode=False)
        except ProgrammingError as e:
            # MySQL is being refreshed and tables are not ready
            # We're getting `Table ... doesn't exist`
            if "doesn't exist" in str(e):
                log.warning("scan-chart-diff.missing-table", pr=pr)
                continue
            else:
                raise e
        except OperationalError as e:
            # PRs without a staging server
            if "Unknown MySQL server host" in str(e):
                log.warning("scan-chart-diff.unknown-host", pr=pr)
                continue
            # PRs with schema migrations
            if "Unknown database" in str(e):
                log.warning("scan-chart-diff.unknown-database", pr=pr)
                continue
            if "Unknown column" in str(e):
                log.warning("scan-chart-diff.unknown-column", pr=pr)
                continue
            # Server is likely not ready yet
            if "Can't connect to MySQL server" in str(e):
                log.warning("scan-chart-diff.cant-connect", pr=pr)
                continue
            else:
                raise e

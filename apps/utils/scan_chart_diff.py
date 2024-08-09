import click
import requests
from rich_click.rich_command import RichCommand
from sqlalchemy.exc import OperationalError, ProgrammingError
from structlog import get_logger

from apps.owidbot.cli import cli as owidbot_cli
from etl import config

config.enable_bugsnag()

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
    active_prs = get_prs_from_repo("etl")

    log.info("scan-chart-diff.init", active_prs=active_prs)

    for pr in active_prs:
        log.info("scan-chart-diff.start", pr=pr)
        args = [f"etl/{pr}", "--services", "chart-diff"]
        if dry_run:
            args.append("--dry-run")

        try:
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


def get_prs_from_repo(repo):
    active_prs = []
    # Start with the first page
    url = f"https://api.github.com/repos/owid/{repo}/pulls?per_page=100"

    while url:
        response = requests.get(url)
        response.raise_for_status()  # To handle HTTP errors
        js = response.json()

        # Collect PR head refs from the current page
        for d in js:
            # only owid PRs
            if d["head"]["label"].startswith("owid:"):
                active_prs.append(d["head"]["ref"])

        # Check for the 'next' page link in the headers
        if "next" in response.links:
            url = response.links["next"]["url"]
        else:
            url = None  # No more pages

    # exclude dependabot PRs
    active_prs = [pr for pr in active_prs if "dependabot" not in pr]

    return active_prs

"""Script to create a snapshot of dataset.

The data for these snapshots has been manually obtained via the GitHub API. We have obtained data on

- Issues: Comments, users that participated, etc.
- Pull Requests: Comments, users that participated, etc.
- Commits: Users that participated, etc.

If you want to retrieve this data again, please look at the script `get_stats.py` in the same folder. You can simply execute it. To run different parts of the script please use the variables at the top of the script EXECUTE_ISSUES, EXECUTE_PRS, EXECUTE_COMMIT.

Run this snapshot script as:

    python snapshots/covid/2024-11-05/github_stats.py \
        --issues gh_stats/issues-20241106211832.csv \
        --issues-comments gh_stats/comments-issues-20241106211832.csv \
        --issues-users gh_stats/users-issues-20241106211832.csv \
        --pr gh_stats/prs-20241106220603.csv \
        --pr-comments gh_stats/comments-prs-20241106220603.csv \
        --pr-users gh_stats/users-prs-20241106220603.csv \
        --commits gh_stats/commits/10800-commits-20241105182054.csv \
        --commits-users gh_stats/commits/10800-users-commits-20241105182054.csv \
        --vax-reporting first_reporting_dates.csv

NOTE: To get data on when countries first reported vaccination data, please refer to get_vax_reporting.py script.
"""

from pathlib import Path
from typing import Optional

import click
from structlog import get_logger

from etl.snapshot import Snapshot

log = get_logger()


# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--issues", type=str, help="File with data on issues.")
@click.option("--issues-comments", type=str, help="File with data on issues comments.")
@click.option("--issues-users", type=str, help="File with data on users that commented in issues.")
@click.option("--pr", type=str, help="File with data on PRs.")
@click.option("--pr-comments", type=str, help="File with data on PR comments.")
@click.option("--pr-users", type=str, help="File with data on users that commented in PRs.")
@click.option("--commits", type=str, help="File with data on commits.")
@click.option("--vax-reporting", type=str, help="File with data on reporting of vaccination data.")
@click.option("--commits-users", type=str, help="File with data on commit users.")
def main(
    upload: bool,
    issues: Optional[str] = None,
    issues_comments: Optional[str] = None,
    issues_users: Optional[str] = None,
    pr: Optional[str] = None,
    pr_comments: Optional[str] = None,
    pr_users: Optional[str] = None,
    commits: Optional[str] = None,
    commits_users: Optional[str] = None,
    vax_reporting: Optional[str] = None,
) -> None:
    snapshot_paths = [
        (issues, "github_stats_issues.csv"),
        (issues_comments, "github_stats_issues_comments.csv"),
        (issues_users, "github_stats_issues_users.csv"),
        (pr, "github_stats_pr.csv"),
        (pr_comments, "github_stats_pr_comments.csv"),
        (pr_users, "github_stats_pr_users.csv"),
        (commits, "github_stats_commits.csv"),
        (commits_users, "github_stats_commits_users.csv"),
        (vax_reporting, "github_stats_vax_reporting.csv"),
    ]

    for paths in snapshot_paths:
        if paths[0] is not None:
            log.info(f"Importing {paths[1]}.")
            # Create a new snapshot.
            snap = Snapshot(f"covid/{SNAPSHOT_VERSION}/{paths[1]}")

            # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
            snap.create_snapshot(filename=paths[0], upload=upload)
        else:
            log.warning(f"Skipping import for {paths[1]}.")


if __name__ == "__main__":
    main()

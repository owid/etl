"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    # Issues
    tb_issues = paths.read_snap_table("github_stats_issues.csv", safe_types=False).sort_values("date_created")
    tb_issues_com = paths.read_snap_table("github_stats_issues_comments.csv", safe_types=False).sort_values(
        "date_created"
    )
    tb_issues_usr = paths.read_snap_table("github_stats_issues_users.csv", safe_types=False).rename(
        columns={
            "index": "user_id",
        }
    )

    # PRs
    tb_pr = paths.read_snap_table("github_stats_pr.csv", safe_types=False).sort_values("date_created")
    tb_pr_com = paths.read_snap_table("github_stats_pr_comments.csv", safe_types=False).sort_values("date_created")
    tb_pr_usr = paths.read_snap_table("github_stats_pr_users.csv", safe_types=False).rename(
        columns={
            "index": "user_id",
        }
    )

    # Commits
    tb_commits = paths.read_snap_table("github_stats_commits.csv", safe_types=False).sort_values("date")
    tb_commits_usr = paths.read_snap_table("github_stats_commits_users.csv", safe_types=False).rename(
        columns={
            "index": "user_id",
        }
    )

    # Join tables
    tables = [
        tb_issues.format(["issue_id"]),
        tb_issues_com.format(["comment_id"]),
        tb_issues_usr.format(["user_id"]),
        tb_pr.format(["issue_id"]),
        tb_pr_com.format(["comment_id"]),
        tb_pr_usr.format(["user_id"]),
        tb_commits.format(["sha"]).sort_values("date"),
        tb_commits_usr.format(["user_id"]),
    ]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tables, check_variables_metadata=True)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

"""Load a meadow dataset and create a garden dataset.


Given: raw tables with data on issues, pull requests and commits.

Goal:
    (higher) User involvement:

    - A) Number of users submitting an issue or PR
    - B) Number of users submitting a commit
    - C = A or B
    - Contributions by country (e.g. number of users, number of user-comments, etc.)

    (lower) User involvement:
    - A) Number of users submitting an issue or PR, or commenting to one
    - B) Number of users submitting a commit
    - C = A or B
    - Contributions by country (e.g. number of users, number of user-comments, etc.)
"""

import owid.catalog.processing as pr

from etl.data_helpers.misc import expand_time_column
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs and pre-process data.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("github_stats")

    # Combine PR & issues tables.
    tb_issues = ds_meadow.read_table("github_stats_issues")
    tb_pr = ds_meadow.read_table("github_stats_pr")
    tb_issues = tb_issues.merge(tb_pr[["issue_id", "is_pr"]], on=["issue_id"], how="outer")
    tb_issues["is_pr"] = tb_issues["is_pr"].fillna(False)
    assert tb_issues.author_login.notna().all(), "Some missing usernames!"

    # Get list of all comments (including issue/pr description)
    tb_comments = ds_meadow.read_table("github_stats_issues_comments")
    tb_comments_pr = ds_meadow.read_table("github_stats_pr_comments")
    tb_comments = pr.concat([tb_comments, tb_comments_pr], ignore_index=True)
    tb_comments = tb_comments.drop_duplicates(subset=["comment_id"])
    assert tb_comments.user_id.notna().all(), "Some missing usernames!"

    # Get the list of all users
    tb_users = ds_meadow.read_table("github_stats_issues_users")
    tb_users_pr = ds_meadow.read_table("github_stats_pr_users")
    tb_users = pr.concat([tb_users, tb_users_pr], ignore_index=True)
    tb_users = tb_users.drop_duplicates(subset=["user_id"])
    assert tb_users.user_login.notna().all(), "Some missing usernames!"

    # Commits
    tb_commits = ds_meadow.read_table("github_stats_commits")

    #
    # Process data.
    #
    # 1/ Number of distinct users submitting an issue or PR over time
    tb_issues["date"] = tb_issues["date_created"].astype("datetime64").dt.date
    tb_distinct_users = tb_issues.sort_values("date").drop_duplicates(subset=["author_login"], keep="first")
    tb_distinct_users = tb_distinct_users.groupby("date", as_index=False).author_login.nunique()
    tb_distinct_users["number_distinct_users"] = tb_distinct_users["author_login"].cumsum()
    tb_distinct_users = expand_time_column(tb_distinct_users, time_col="date", fillna_method="ffill")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

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

Clarifications:

    - github_stats_issues: list of all issues, including PRs.
    - github_stats_pr: list of PRs (redundant with `issues`)
    - github_stats_issues_comments:  list of comments on issues.
    - github_stats_pr_comments: list of comments on PRs. These are not regular comments, but comments on code (e.g. review comments).

"""

import owid.catalog.processing as pr
import pandas as pd

from etl.data_helpers.misc import expand_time_column
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Other config
COLNAME_BASE = "number_distinct_users"


def run(dest_dir: str) -> None:
    #
    # Load inputs and pre-process data.
    #
    # 1/ Load meadow dataset.
    ds_meadow = paths.load_dataset("github_stats")

    # Combine PR & issues tables.
    tb_issues = ds_meadow.read("github_stats_issues")
    # tb_pr = ds_meadow.read("github_stats_pr")
    tb_issues = make_table_issues(tb_issues)

    # Get list of all comments (including issue/pr description)
    tb_comments = ds_meadow.read("github_stats_issues_comments")
    tb_comments_pr = ds_meadow.read("github_stats_pr_comments")
    tb_comments = make_table_comments(tb_comments, tb_comments_pr)

    # Get the list of all users
    tb_users = ds_meadow.read("github_stats_issues_users")
    tb_users_pr = ds_meadow.read("github_stats_pr_users")
    tb_users = pr.concat([tb_users, tb_users_pr], ignore_index=True)
    tb_users = tb_users.drop_duplicates(subset=["user_id"])
    assert tb_users.user_login.notna().all(), "Some missing usernames!"

    # # Commits
    # tb_commits = ds_meadow.read("github_stats_commits")

    #
    # Process data.
    #
    # 1/ TABLE: user contributions
    ## Get table with number of new users contribution to the repository
    tb_distinct_users = make_table_user_counts(tb_issues, tb_comments, tb_users)
    ## Add flavours of counts (cumulative, weekly, 7-day rolling sum, etc.)
    tb_distinct_users = get_intervals(tb_distinct_users)

    # 2/ TABLE: issues or PR created, and comments
    ## Issue or PR
    tb_issues_count = tb_issues.copy()
    tb_issues_count["new_pr"] = tb_issues_count["is_pr"].astype(int)
    tb_issues_count["new_issue"] = (~tb_issues_count["is_pr"]).astype(int)
    tb_issues_count["new_issue_or_pr"] = 1
    tb_issues_count["new_issue_or_pr"] = tb_issues_count["new_issue_or_pr"].copy_metadata(tb_issues_count["new_issue"])
    tb_issues_count = tb_issues_count.groupby("date", as_index=False)[["new_issue", "new_pr", "new_issue_or_pr"]].sum()
    ## Comments
    tb_comments_count = tb_comments.copy()
    tb_comments_count["new_comment_issue_or_pr"] = 1
    tb_comments_count["new_comment_issue_or_pr"] = tb_comments_count["new_comment_issue_or_pr"].copy_metadata(
        tb_comments_count["issue_id"]
    )
    tb_comments_count = tb_comments_count.groupby("date", as_index=False)["new_comment_issue_or_pr"].sum()
    ## Combine
    tb_counts = tb_issues_count.merge(tb_comments_count, on="date", how="outer")
    tb_counts = expand_time_column(tb_counts, time_col="date", fillna_method="zero")
    tb_counts["new_contributions"] = tb_counts["new_issue_or_pr"] + tb_counts["new_comment_issue_or_pr"]
    tb_counts = tb_counts.format("date")
    ## Intervals
    tb_counts = get_intervals(tb_counts)

    # 4/ Format
    tb_distinct_users = tb_distinct_users.format(["date", "interval"], short_name="user_contributions").astype(int)
    tb_counts = tb_counts.format(["date", "interval"], short_name="contributions").astype(int)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    tables = [
        tb_distinct_users,
        tb_counts,
    ]

    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def make_table_issues(tb_issues):
    assert tb_issues.author_login.notna().all(), "Some missing usernames!"
    ## Add date
    ## Dtypes
    tb_issues = tb_issues.astype(
        {
            "author_name": "string",
            "author_login": "string",
            "date_created": "datetime64[ns]",
            "is_pr": "bool",
        }
    )
    tb_issues["date_created"] = pd.to_datetime(tb_issues["date_created"])
    tb_issues["date"] = pd.to_datetime(tb_issues["date_created"].dt.date)

    ## Sort
    tb_issues = tb_issues.sort_values("date_created")

    # Columns
    tb_issues = tb_issues[
        [
            "issue_id",
            "author_name",
            "author_login",
            "date_created",
            "date",
            "is_pr",
        ]
    ]
    return tb_issues


def make_table_comments(tb_issues, tb_pr):
    tb_pr["is_pr"] = True
    tb = pr.concat([tb_issues, tb_pr], ignore_index=True)
    tb["is_pr"] = tb["is_pr"].fillna(False)

    assert tb["comment_id"].value_counts().max(), "Repeated comments!"
    assert tb.user_id.notna().all(), "Some missing usernames!"

    tb = tb.astype(
        {
            "date_created": "datetime64[ns]",
            "date_updated": "datetime64[ns]",
            "is_pr": "bool",
        }
    )
    tb["date"] = pd.to_datetime(tb["date_created"].dt.date)

    # Sort rows and columns
    tb = tb.sort_values(["issue_id", "date"])[
        [
            "comment_id",
            "issue_id",
            "date",
            "date_created",
            "date_updated",
            "user_id",
            "is_pr",
        ]
    ]
    return tb


def get_number_distinct_users(tb, col_pr_flag, colname_user, colname_output, col_date: str = "date"):
    def _get_counts(tb, colname_output):
        # Drop duplicate users
        tb = tb.drop_duplicates(subset=[colname_user], keep="first")

        # Get unique number for a given date
        tb = tb.groupby(col_date, as_index=False)[colname_user].nunique()

        # Drop unnecessary columns
        tb = tb.rename(columns={colname_user: colname_output})

        return tb

    tb_pr = _get_counts(tb[tb[col_pr_flag]], f"{colname_output}_pr")
    tb_issue = _get_counts(tb[~tb[col_pr_flag]], f"{colname_output}_issue")
    tb_any = _get_counts(tb, f"{colname_output}_any")

    tb = pr.multi_merge([tb_pr, tb_issue, tb_any], on=col_date, how="outer").fillna(0)

    # Fill NaNs and set dtypes
    columns = [col for col in tb.columns if col != col_date]
    tb[columns] = tb[columns].fillna(0).astype("Int64")

    return tb


def combine_user_contribution(tb_create, tb_comment, tb_any):
    tb = pr.multi_merge([tb_create, tb_comment, tb_any], on="date", how="outer")
    tb = expand_time_column(df=tb, time_col="date", fillna_method="zero")
    tb = tb.format("date")
    return tb


def get_intervals(tb):
    ## 4.1/ Cumulative
    tb_cum = tb.cumsum().reset_index()
    tb_cum["interval"] = "cumulative"

    ## 4.3/ Weekly
    tb_week = tb.resample("W").sum().reset_index()
    tb_week["interval"] = "weekly"

    ## 4.3/ 4-week
    tb_4week = tb.resample("4W").sum().reset_index()
    tb_4week["interval"] = "4-weekly"

    ## 4.4/ 7-day rolling
    tb_rolling = tb.rolling(window=7, min_periods=0).sum().reset_index()
    tb_rolling["interval"] = "7-day rolling sum"

    ## 4.5/ Combine
    tb = pr.concat([tb_cum, tb_rolling, tb_week, tb_4week])

    return tb


def make_table_user_counts(tb_issues, tb_comments, tb_users):
    # 2.1/ Number of distinct users submitting an issue or PR over time
    tb_distinct_users_create = get_number_distinct_users(tb_issues, "is_pr", "author_login", f"{COLNAME_BASE}_create")

    # 2.2/ Number of distinct users commenting in an issue or PR thread
    tb_distinct_users_comments = get_number_distinct_users(tb_comments, "is_pr", "user_id", f"{COLNAME_BASE}_comment")

    # 2.3 Any
    tb_issues_b = tb_issues.merge(
        tb_users[["user_login", "user_id"]], left_on="author_login", right_on="user_login", how="left"
    )
    cols = ["date", "user_id", "issue_id", "is_pr"]
    tb_any = pr.concat([tb_issues_b.loc[:, cols], tb_comments.loc[:, cols]])
    tb_distinct_users_any = get_number_distinct_users(tb_any, "is_pr", "user_id", f"{COLNAME_BASE}")

    # 3/ Combine
    tb = combine_user_contribution(tb_distinct_users_create, tb_distinct_users_comments, tb_distinct_users_any)

    return tb

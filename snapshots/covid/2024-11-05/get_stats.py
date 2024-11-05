# issues

# issue_id, author_name, author_login, date_created
# issue.id, issue.user.name, issue.user.login, issue.created_at


# comments

# comment_id, date_created, date_updated, user_id, issue_id
# comment.id, comment.created_at, comment.updated_at, user_id, issue.id


# users

# user_id, user_login, user_name, user_location
# user.id, user.login, user.name, user.location


from datetime import datetime
from typing import Optional

import github
import github.PullRequest
import github.Repository
import pandas as pd
from github import Auth, Github

from etl import config

# FLAGS
EXECUTE_ISSUES = False
EXECUTE_PRS = False
EXECUTE_COMMIT = True


def get_repo(repo_name: str, access_token: Optional[str] = None) -> github.Repository.Repository:
    """Get repository."""
    if not access_token:
        assert config.OWIDBOT_ACCESS_TOKEN, "OWIDBOT_ACCESS_TOKEN is not set"
        access_token = config.OWIDBOT_ACCESS_TOKEN
    auth = Auth.Token(access_token)
    g = Github(auth=auth)
    return g.get_repo(f"owid/{repo_name}")


def process_issue(issue_or_pr, users):
    """Function to process each issue and its comments."""
    issue_or_pr_data = {
        "issue_id": issue_or_pr.number,
        "author_name": issue_or_pr.user.name,
        "author_login": issue_or_pr.user.login,
        "date_created": issue_or_pr.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "is_pr": "pull/" in issue_or_pr.html_url,
    }
    issue_or_pr_comments = []

    for comment in issue_or_pr.get_comments():
        user = comment.user
        issue_or_pr_comments.append(
            {
                "comment_id": comment.id,
                "date_created": comment.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "date_updated": comment.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                "user_id": user.id,
                "issue_id": issue_or_pr.number,
            }
        )

        if user.id not in users:
            users[user.id] = {
                "user_login": user.login,
                "user_name": user.name,
                "user_location": user.location,
            }

    return issue_or_pr_data, issue_or_pr_comments, users


# Get repository
repo = get_repo("covid-19-data", access_token=config.GITHUB_TOKEN)


######################################################
# GET DATA FOR ISSUES
######################################################
if EXECUTE_ISSUES:
    # Initialize lists (we will store output data here)
    issues = []
    comments = []
    users = {}

    # Get issues
    issues_raw = repo.get_issues(state="all")
    total_issues = issues_raw.totalCount  # Total number of issues for progress tracking

    # Retrieve relevant data (several API calls)
    for i, issue in enumerate(issues_raw):
        issue_data, issue_comments, users = process_issue(issue, users)
        issues.append(issue_data)
        comments.extend(issue_comments)
        print(f"Progress: {i}/{total_issues} issues processed")

    # Export
    rand = str(datetime.now().strftime("%Y%m%d%H%M%S"))
    pd.DataFrame(comments).to_csv(f"gh_stats/comments-issues-{rand}.csv", index=False)
    pd.DataFrame(issues).to_csv(f"gh_stats/issues-{rand}.csv", index=False)
    pd.DataFrame(users).T.reset_index().to_csv(f"gh_stats/users-issues-{rand}.csv", index=False)

######################################################
# GET DATA FOR PRS
######################################################
if EXECUTE_PRS:
    # Initialize lists (we will store output data here)
    prs = []
    comments = []
    users = {}

    # Get PRs
    prs_raw = repo.get_pulls(state="all")
    total_prs = prs_raw.totalCount  # Total number of PRs for progress tracking

    # Retrieve relevant data (several API calls)
    for i, pr in enumerate(prs_raw):
        pr_data, issue_comments, users = process_issue(pr, users)
        prs.append(pr_data)
        comments.extend(issue_comments)
        print(f"Progress: {i}/{total_prs} PRs processed")

    # Export
    rand = str(datetime.now().strftime("%Y%m%d%H%M%S"))
    pd.DataFrame(comments).to_csv(f"gh_stats/comments-prs-{rand}.csv", index=False)
    pd.DataFrame(prs).to_csv(f"gh_stats/prs-{rand}.csv", index=False)
    pd.DataFrame(users).T.reset_index().to_csv(f"gh_stats/users-prs-{rand}.csv", index=False)

######################################################
# GET DATA FOR COMMITS
######################################################
if EXECUTE_COMMIT:
    # Initialize lists (we will store output data here)
    commits = []
    users = {}

    # Get commits
    commits_raw = repo.get_commits()
    total_commits = commits_raw.totalCount  # Total number of commits for progress tracking

    # Retrieve relevant data (several API calls)
    for i, c in enumerate(commits_raw):
        if i % 10 == 0:
            print(f"Progress: {i}/{total_commits} commits processed")
        user = c.author
        commit_raw = {
            "sha": c.sha,
            "date": c.commit.author.date.strftime("%Y-%m-%d %H:%M:%S"),
            "files_changed": len(c.files),
            "lines_changed": c.stats.total,
            "lines_deleted": c.stats.deletions,
            "lines_added": c.stats.additions,
            "user_id": user.id,
        }
        commits.append(commit_raw)
        # Add user
        if user.id not in users:
            users[user.id] = {
                "user_login": user.login,
                "user_name": user.name,
                "user_location": user.location,
            }

        if (i != 0) & (i % 50 == 0):
            # Export
            print(f"Exporting {i}...")
            rand = str(datetime.now().strftime("%Y%m%d%H%M%S"))
            pd.DataFrame(commits).to_csv(f"gh_stats/commits/{i}-commits-{rand}.csv", index=False)
            pd.DataFrame(users).T.reset_index().to_csv(f"gh_stats/commits/{i}-users-commits-{rand}.csv", index=False)

    # Export
    rand = str(datetime.now().strftime("%Y%m%d%H%M%S"))
    pd.DataFrame(commits).to_csv(f"gh_stats/commits/total-commits-{rand}.csv", index=False)
    pd.DataFrame(users).T.reset_index().to_csv(f"gh_stats/commits/total-users-commits-{rand}.csv", index=False)

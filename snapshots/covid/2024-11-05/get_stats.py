"""
issues: list of all issues, including PRs.
pr: list of PRs (redundant with `issues`)

issues_comments:  list of comments on issues.
pr_comments: list of comments on PRs. These are not regular comments, but comments on code (e.g. review comments).
"""

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
EXECUTE_COMMIT = False
SKIP_COMMITS = 15_400  # 10_700


def get_repo(
    repo_name: str, access_token: Optional[str] = None, per_page: Optional[int] = None
) -> github.Repository.Repository:
    """Get repository."""
    if not access_token:
        assert config.OWIDBOT_ACCESS_TOKEN, "OWIDBOT_ACCESS_TOKEN is not set"
        access_token = config.OWIDBOT_ACCESS_TOKEN
    auth = Auth.Token(access_token)
    if per_page:
        g = Github(auth=auth, per_page=per_page)
    else:
        g = Github(auth=auth)
    return g.get_repo(f"owid/{repo_name}")


def process_issue(issue_or_pr, users):
    """Function to process each issue and its comments."""
    is_pr = "pull/" in issue_or_pr.html_url
    user = issue_or_pr.user
    issue_or_pr_data = {
        "issue_id": issue_or_pr.number,
        "author_name": user.name,
        "author_login": user.login,
        "date_created": issue_or_pr.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "is_pr": is_pr,
    }
    issue_or_pr_comments = []

    if user.id not in users:
        users[user.id] = {
            "user_login": user.login,
            "user_name": user.name,
            "user_location": user.location,
        }

    for comment in issue_or_pr.get_comments():
        user = comment.user
        issue_or_pr_comments.append(
            {
                "comment_id": comment.id,
                "date_created": comment.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "date_updated": comment.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                "user_id": user.id,
                "issue_id": issue_or_pr.number,
                "is_pr": is_pr,
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


if SKIP_COMMITS != 0:
    PER_PAGE = 100
    repo = get_repo("covid-19-data", access_token=config.GITHUB_TOKEN, per_page=PER_PAGE)

    # Initialize lists (we will store output data here)
    commits = []
    users = {}

    # Get commits
    commits_raw = repo.get_commits()
    total_commits = commits_raw.totalCount  # Total number of commits for progress tracking

    # Calculate the starting page
    start_page = (SKIP_COMMITS // PER_PAGE) + 1
    end_page = (total_commits // PER_PAGE) + 1

    # Initialize a list to store commits from the 101st onward
    commits = []

    # Fetch commits from the 101st onward
    for page in range(start_page, end_page):  # Adjust the range as needed
        print(f"> Progress: {page}/{end_page} commit pages processed ({PER_PAGE * page} commits)")
        commit_page = repo.get_commits().get_page(page)
        if not commit_page:
            break  # Stop if there are no more commits
        # Retrieve relevant data (several API calls)
        for i, c in enumerate(commit_page):
            if i % 10 == 0:
                print(f">> Progress: {i}/{PER_PAGE} commits processed")

            user = c.committer
            stats = c.stats

            commit_raw = {
                "sha": c.sha,
                "date": c.commit.author.date.strftime("%Y-%m-%d %H:%M:%S"),
                "files_changed": len(c.files),
                "lines_changed": stats.total,
                "lines_deleted": stats.deletions,
                "lines_added": stats.additions,
            }

            if user is None:
                commit_raw["user_id"] = c.commit.author.email
            else:
                commit_raw["user_id"] = user.id

            commits.append(commit_raw)
            # Add user
            if user is None:
                if c.commit.author.email not in users:
                    users[c.commit.author.email] = {
                        "user_login": None,
                        "user_name": c.commit.author.name,
                        "user_location": None,
                    }
            else:
                # print(user)
                if user.id not in users:
                    try:
                        users[user.id] = {
                            "user_login": user.login,
                            "user_name": user.name,
                            "user_location": user.location,
                        }
                    except Exception:
                        users[user.id] = {
                            "user_login": user.login,
                            "user_name": None,
                            "user_location": None,
                        }

            if (i != 0) & (i % 50 == 0):
                # Export
                print(f"Exporting {i}...")
                rand = str(datetime.now().strftime("%Y%m%d%H%M%S"))
                pd.DataFrame(commits).to_csv(f"gh_stats/commits/{PER_PAGE * page}-{i}-commits-{rand}.csv", index=False)
                pd.DataFrame(users).T.reset_index().to_csv(
                    f"gh_stats/commits/{PER_PAGE * page}-{i}-users-commits-{rand}.csv", index=False
                )

    # Export
    rand = str(datetime.now().strftime("%Y%m%d%H%M%S"))
    pd.DataFrame(commits).to_csv(f"gh_stats/commits/total-commits-{rand}.csv", index=False)
    pd.DataFrame(users).T.reset_index().to_csv(f"gh_stats/commits/total-users-commits-{rand}.csv", index=False)

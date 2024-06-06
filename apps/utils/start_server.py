"""This script creates a new git branch, makes an empty commit, and creates a draft pull request.

Usually, this script will be executed from the master branch on your local ETL repos, to create a new branch from there.
But you can also run it from any other existing branch, to create a sub-branch.

The resulting draft PR will be new_branch_name -> base_branch.
* The new_branch_name is the name of the new branch to be created (that must be given as an input).
* The base branch (if not given as an input) will be the current branch, where your ETL repos currently is.

To create a Github token, go to github settings. On the left, at the bottom, choose Developer settings.
Then, choose "Personal access tokens" and "Tokens (classic)". Click on "Generate new token (classic)".
Give it a name (e.g. "etl-work"), set an expiration time, and select the scope "repo".
Then, click on Generate token. Copy the token and save it in your .env file as GITHUB_TOKEN.
"""

from typing import Optional

import click
import requests
from git import Repo
from rich_click.rich_command import RichCommand
from structlog import get_logger

from etl.config import GITHUB_TOKEN
from etl.paths import BASE_DIR

# Initialize logger.
log = get_logger()

# URL of the Github API, to be used to create a draft PR in the ETL repos.
GITHUB_API_URL = "https://api.github.com/repos/owid/etl/pulls"


@click.command(name="start-server", cls=RichCommand, help=__doc__)
@click.argument("new-branch-name", type=str)
@click.option(
    "--base-branch", type=str, default=None, help="Name of base branch (if not given, current branch will be used)."
)
def cli(new_branch_name: str, base_branch: Optional[str] = None) -> None:
    if not GITHUB_TOKEN:
        log.error("Define GITHUB_TOKEN in your ETL .env file. Then run this tool again.")
        return

    # Initialize a repos object at the root folder of the etl repos.
    repo = Repo(BASE_DIR)

    if base_branch is None:
        # Assume that the current branch will be the the base branch.
        # The resulting draft PR will be branch_name -> base_branch.
        base_branch = repo.active_branch.name

    # Ensure the base branch exists in remote.
    origin = repo.remote(name="origin")
    # Update the list of remote branches.
    origin.fetch()
    # NOTE: For some reason, remote_branches includes branches that don't seem to exist anymore in github.
    remote_branches = [ref.name.split("/")[-1] for ref in origin.refs if ref.remote_head != "HEAD"]
    if base_branch not in remote_branches:
        log.error(
            f"Branch {base_branch} does not exist in remote. "
            "Either push current branch (git push origin current-branch-name) or go to master (git checkout master). "
            "Then run this tool again."
        )
        return

    # Ensure the new branch does not already exist.
    if new_branch_name in [branch.name for branch in repo.branches]:
        log.error(f"Branch {new_branch_name} already exists. Choose a different name for the new branch to be created.")
        return

    # Create new branch and switch to it.
    repo.git.checkout("-b", new_branch_name)

    log.info("Creating an empty commit.")
    repo.git.commit("--allow-empty", "-m", "Start a new staging server")

    log.info("Pushing the new branch to remote.")
    repo.git.push("origin", new_branch_name)

    log.info("Creating a draft pull request.")
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    data = {
        "title": f":construction: Draft PR for {new_branch_name}",
        "head": new_branch_name,
        "base": base_branch,
        "body": "",
        "draft": True,
    }
    response = requests.post(GITHUB_API_URL, json=data, headers=headers)
    if response.status_code == 201:
        log.info("Draft pull request created successfully.")
    else:
        log.error(f"Failed to create draft pull request:\n{response.content}")

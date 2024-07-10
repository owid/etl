"""This script creates a new draft pull request in GitHub, which starts a new staging server.

Usages:

- If you have already created a new local branch, run this script without specifying any argument. The 'master' branch will be assumed as the base branch of the new draft pull request.
- If you are in the master branch on your local ETL repo, pass the name of the new branch as an argument. This will create a new local branch, which will be pushed to remote to create a new draft pull request.
- If you want the base branch to be different from 'master', specify it with the --base-branch argument.
- Use --category, and --title to further customize the title of the PR.

The resulting draft pull request will be new_branch -> base_branch.

"""

from typing import Optional

import click
import requests
from git import GitCommandError, Repo
from rich_click.rich_command import RichCommand
from structlog import get_logger

from etl.config import GITHUB_TOKEN
from etl.paths import BASE_DIR

# Initialize logger.
log = get_logger()

# URL of the Github API, to be used to create a draft pull request in the ETL repos.
GITHUB_API_URL = "https://api.github.com/repos/owid/etl/pulls"
# Add EMOJIs for each PR type
PR_CATEGORIES = {
    "data": {
        "emoji": "📊",
        "emoji_raw": ":bar_chart:",
        "description": "data update or addition",
    },
    "bug": {
        "emoji": "🐛",
        "emoji_raw": ":bug:",
        "description": "bug fix for the user",
    },
    "refactor": {
        "emoji": "🔨",
        "emoji_raw": ":hammer:",
        "description": "a code change that neither fixes a bug nor adds a feature for the user",
    },
    "enhance": {
        "emoji": "✨",
        "emoji_raw": ":sparkles:",
        "description": "visible improvement over a current implementation without adding a new feature or fixing a bug",
    },
    "feature": {
        "emoji": "🎉",
        "emoji_raw": ":tada:",
        "description": "new feature for the user",
    },
    "docs": {
        "emoji": "📜",
        "emoji_raw": ":scroll:",
        "description": "documentation only changes",
    },
    "chore": {
        "emoji": "🧹",
        "emoji_raw": ":honeybee:",
        "description": "upgrading dependencies, tooling, etc. No production code change",
    },
    "style": {
        "emoji": "💄",
        "emoji_raw": ":lipstick:",
        "description": "formatting, missing semi colons, etc. No production code change",
    },
    "wip": {
        "emoji": "🚧",
        "emoji_raw": ":construction:",
        "description": "work in progress - intermediate commits that will be explained later on",
    },
    "tests": {
        "emoji": "✅",
        "emoji_raw": ":white_check_mark:",
        "description": "adding missing tests, refactoring tests, etc. No production code change",
    },
}
description = "- " + "\n- ".join(
    f"**{choice}**: {choice_params['description']}" for choice, choice_params in PR_CATEGORIES.items()
)


@click.command(name="draft-pr", cls=RichCommand, help=__doc__)
@click.argument(
    "new-branch",
    type=str,
    default=None,
    required=False,
)
@click.option(
    "--base-branch", "-b", type=str, default=None, help="Name of base branch (if not given, 'master' will be used)."
)
@click.option("--title", "-t", type=str, default=None, help="Title of the PR and the first commit.")
@click.option(
    "--category",
    "-c",
    type=click.Choice(list(PR_CATEGORIES.keys()), case_sensitive=False),
    help=f"Category of the PR (only relevant if --title is given). A corresponding emoji will be prepended to the title.\n {description}",
)
@click.option(
    "--scope",
    "-s",
    help="Scope of the PR (only relevant if --title is given). This text will be preprended to the PR title. \n\n\n**Examples**: 'demography' for data work on this field, 'etl.db' if working on specific modules, 'wizard', etc.",
)
def cli(
    new_branch: Optional[str] = None,
    base_branch: Optional[str] = None,
    title: Optional[str] = None,
    category: Optional[str] = None,
    scope: Optional[str] = None,
) -> None:
    if not GITHUB_TOKEN:
        log.error(
            """A github token is needed. To create one:
- Go to: https://github.com/settings/tokens
- Click on the dropdown "Generate new token" and select "Generate new token (classic)".
- Give the token a name (e.g., "etl-work"), set an expiration time, and select the scope "repo".
- Click on "Generate token".
- Copy the token and save it in your .env file as GITHUB_TOKEN.
- Run this tool again.
"""
        )
        return

    # Initialize a repos object at the root folder of the etl repos.
    repo = Repo(BASE_DIR)

    # List all local branches.
    local_branches = [branch.name for branch in repo.branches]

    # Create title
    title = generate_pr_title(title, category, scope)

    # Update the list of remote branches in the local repository.
    origin = repo.remote(name="origin")
    origin.fetch()
    # List all remote branches.
    remote_branches = [ref.name.split("origin/")[-1] for ref in origin.refs if ref.remote_head != "HEAD"]

    if base_branch is None:
        # If a base branch is not specified, assume that it will be 'master'.
        base_branch = "master"

    # Ensure the base branch exists in remote (this should always be true for 'master').
    if base_branch not in remote_branches:
        log.error(
            f"Base branch '{base_branch}' does not exist in remote. "
            "Either push that branch (git push origin base-branch-name) or use 'master' as a base branch. "
            "Then run this tool again."
        )
        return

    if new_branch is None:
        # If not explicitly given, the new branch will be the current branch.
        new_branch = repo.active_branch.name
        if new_branch == "master":
            log.error(
                "You're currently on 'master' branch. Pass the name of a branch as an argument to create a new branch."
            )
            return
    else:
        # Ensure the new branch does not already exist locally.
        if new_branch in local_branches:
            log.error(
                f"Branch '{new_branch}' already exists locally."
                "Either choose a different name for the new branch to be created, "
                "or switch to the new branch and run this tool without specifying a new branch."
            )
            return
        try:
            log.info(
                f"Switching to base branch '{base_branch}', creating new branch '{new_branch}' from there, and switching to it."
            )
            repo.git.checkout(base_branch)
            repo.git.checkout("-b", new_branch)
        except GitCommandError as e:
            log.error(f"Failed to create a new branch from '{base_branch}':\n{e}")
            return

    # Ensure the new branch does not already exist in remote.
    if new_branch in remote_branches:
        log.error(
            f"New branch '{new_branch}' already exists in remote. "
            "Either manually create a pull request from github, or use a different name for the new branch."
        )
        return

    log.info("Creating an empty commit.")
    repo.git.commit("--allow-empty", "-m", title or f"Start a new staging server for branch '{new_branch}'")

    log.info("Pushing the new branch to remote.")
    repo.git.push("origin", new_branch)

    log.info("Creating a draft pull request.")
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    data = {
        "title": title or f":construction: Draft PR for branch {new_branch}",
        "head": new_branch,
        "base": base_branch,
        "body": "",
        "draft": True,
    }
    response = requests.post(GITHUB_API_URL, json=data, headers=headers)
    if response.status_code == 201:
        js = response.json()
        log.info(f"Draft pull request created successfully at {js['html_url']}.")
    else:
        log.error(f"Failed to create draft pull request:\n{response.json()}")


def generate_pr_title(title: str | None, category: str | None, scope: str | None) -> None | str:
    """Generate the PR title.

    title + category + scope -> 'category scope: title'
    title + category -> 'category title'
    scope + title -> 'scope: title'
    """
    if title is not None:
        prefix = ""
        # Add emoji for PR mode chosen if applicable
        if category is not None:
            if category in PR_CATEGORIES:
                prefix = PR_CATEGORIES[category]["emoji"]
            else:
                log.error(f"Invalid PR type '{category}'. Choose one of {list(PR_CATEGORIES.keys())}.")
                return
        # Add scope
        if scope is not None:
            if prefix != "":
                prefix += " "
            prefix += f"{scope}:"

        # Add prefix
        title = f"{prefix} {title}"
    return title

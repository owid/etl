"""This script creates a new draft pull request in GitHub, which starts a new staging server.

Arguments:

`TITLE`: The title of the PR. This must be given.

`CATEGORY`: The category of the PR. This is optional. If not given, the user will be prompted to choose one.

**Main use case**: Branch out from `master` to a temporary `work_branch`, and create a PR to merge `work_branch` -> `master`. You will be asked to choose a category. The value of `work_branch` will be auto-generated based on the title and the category.

```shell
# Without specifying a category (you will be prompted for a category)
etl pr "some title for the PR"

# With a category
etl pr "some title for the PR" data

# With private stating server
etl pr "some title for the PR" --private
```

**Custom use case (1)**: Same as main use case, but with a specific branch name for the `work_branch`.

```shell
etl pr "some title for the PR" --work-branch "this-temporary-branch"
# Shorter
etl pr "some title for the PR" -w "this-temporary-branch"
```

**Custom use case (2)**: Create a pull request from `current_branch` to `master`.

```shell
etl pr "some title for the PR" --direct
```

**Custom use case (3)**: Create a pull request from branch `this-temporary-branch` -> `develop`.

```shell
etl pr "some title for the PR" --direct --base-branch "develop" --work-branch "this-temporary-branch"
# Shorter
etl pr "some title for the PR" --direct -b "develop" -w "this-temporary-branch"
```
"""

import hashlib
import os
import re
import uuid
from typing import Optional, cast

import click
import questionary
import requests
from git import GitCommandError, Repo
from rich_click.rich_command import RichCommand
from structlog import get_logger

from apps.pr.categories import PR_CATEGORIES, PR_CATEGORIES_CHOICES
from apps.utils.gpt import OpenAIWrapper
from etl.config import GITHUB_TOKEN
from etl.paths import BASE_DIR

# Initialize logger.
log = get_logger()

# Style for questionary
SHELL_FORM_STYLE = questionary.Style(
    [
        ("qmark", "fg:#fac800 bold"),  # token in front of the question
        ("question", "bold"),  # question text
        ("answer", "fg:#fac800 bold"),  # submitted answer text behind the question
        ("pointer", "fg:#fac800 bold"),  # pointer used in select and checkbox prompts
        ("highlighted", "bg:#fac800 fg:#000000 bold"),  # pointed-at choice in select and checkbox prompts
        ("selected", "fg:#54cc90"),  # style for a selected item of a checkbox
        ("separator", "fg:#cc5454"),  # separator in lists
        # ('instruction', ''),                # user instructions for select, rawselect, checkbox
        ("text", ""),  # plain text
        # ('disabled', 'fg:#858585 italic')   # disabled choices for select and checkbox prompts
    ]
)


@click.command(
    name="pr",
    cls=RichCommand,
    help=__doc__,
)
@click.argument(
    "title",
    type=str,
    required=True,
)
@click.argument(
    "category",
    type=click.Choice(list(PR_CATEGORIES.keys()), case_sensitive=False),
    required=False,
    default=None,
)
@click.option(
    "--scope",
    "-s",
    help="Scope of the PR (only relevant if --title is given). This text will be preprended to the PR title. **Examples**: 'demography' for data work on this field, 'etl.db' if working on specific modules, 'wizard', etc.",
    default=None,
)
@click.option(
    "--work-branch",
    "-w",
    "work_branch",
    type=str,
    default=None,
    help="The name of the work branch to create. It is auto-generated based on the title and the category. If --direct is used, this is the PR source branch and defaults to the current branch.",
)
@click.option(
    "--base-branch",
    "-b",
    "base_branch",
    type=str,
    default="master",
    help="Name of the base branch. This is the branch to branch out from and merge back into. If --direct is used, this is the PR target branch.",
)
@click.option(
    "--direct",
    "-d",
    is_flag=True,
    help="Directly create a PR from the current branch to the target branch (default: master).",
)
@click.option(
    "--private",
    "-p",
    is_flag=True,
    help="By default, staging server site (not admin) will be publicly accessible. Use --private to have it private instead. This does not apply when using --direct mode.",
)
@click.option(
    "--no-llm",
    "-n",
    is_flag=True,
    help="We briefly use LLMs to simplify the title and use it in the branch name. Disable this by using -n flag.",
)
def cli(
    title: str,
    category: Optional[str],
    scope: Optional[str],
    work_branch: Optional[str],
    base_branch: str,
    direct: bool,
    private: bool,
    no_llm: bool,
    # base_branch: Optional[str] = None,
) -> None:
    # Check that the user has set up a GitHub token.
    check_gh_token()

    # Validate title
    _validate_title(title)

    # Get category
    category = ensure_category(category)

    # Create title
    pr_title = PRTitle(
        title=title,
        category=category,
        scope=scope,
    )

    # Initialize repository, get remote branches
    repo, remote_branches = init_repo()

    # Get the new branch
    work_branch = ensure_work_branch(
        repo=repo,
        work_branch=work_branch,
        direct=direct,
        pr_title=pr_title,
        remote_branches=remote_branches,
        no_llm=no_llm,
    )

    # Check branches main & work make sense!
    check_branches_valid(base_branch, work_branch, remote_branches)

    # Auto PR mode: Create a new branch from the base branch.
    if not direct:
        if private:
            if not work_branch.endswith("-private"):
                work_branch = f"{work_branch}-private"
        branch_out(repo, base_branch, work_branch)

    # Create PR
    create_pr(repo, work_branch, base_branch, pr_title)


def check_gh_token():
    if not GITHUB_TOKEN:
        raise click.ClickException(
            """A github token is needed. To create one:
- Go to: https://github.com/settings/tokens
- Click on the dropdown "Generate new token" and select "Generate new token (classic)".
- Give the token a name (e.g., "etl-work"), set an expiration time, and select the scope "repo".
- Click on "Generate token".
- Copy the token and save it in your .env file as GITHUB_TOKEN.
- Run this tool again.
"""
        )


def _validate_title(title):
    if not bool(re.search(r"\w+", title)):
        raise click.ClickException("Invalid title! Use at least one word!")


def ensure_category(category: Optional[str]):
    """Get category if not provided."""
    if category is None:
        # show suggestions
        choices = [questionary.Choice(**choice) for choice in PR_CATEGORIES_CHOICES]  # type: ignore
        category = questionary.select(
            message="Please choose a PR category",
            choices=choices,
            use_shortcuts=True,
            style=SHELL_FORM_STYLE,
            instruction="(Use shortcuts or arrow keys)",
        ).unsafe_ask()

    category = cast(str, category)

    return category


class PRTitle:
    def __init__(self, title, category, scope):
        self.title = title
        self.category = category
        self.scope = scope

    def __str__(self) -> str:
        title_actual = _generate_pr_title(self.title, self.category, self.scope)
        if title_actual is None:
            raise click.ClickException("Failed to generate PR title.")
        return title_actual


def init_repo():
    # Initialize a repos object at the root folder of the etl repos.
    repo = Repo(BASE_DIR)
    # Update the list of remote branches in the local repository.
    origin = repo.remote(name="origin")
    # NOTE: The option prune=True removes local references to branches that no longer exist on the remote repository.
    #  Otherwise, this script might raise an error claiming that your proposed branch exists in remote, even if that
    #  branch was already deleted.
    origin.fetch(prune=True)
    # List all remote branches.
    remote_branches = [ref.name.split("origin/")[-1] for ref in origin.refs if ref.remote_head != "HEAD"]

    return repo, remote_branches


def ensure_work_branch(repo, work_branch, direct, pr_title, remote_branches, no_llm):
    """Get name of new branch if not provided."""
    # If no name for new branch is given
    if work_branch is None:
        if not direct:
            # Generate name for new branch
            work_branch = bake_branch_name(repo, pr_title, no_llm, remote_branches)
        else:
            # If not explicitly given, the new branch will be the current branch.
            work_branch = repo.active_branch.name
            if work_branch == "master":
                message = "You're currently on 'master' branch. Pass the name of a branch as an argument to create a new branch."
                raise click.ClickException(message)
    # If a name is given, and not in direct mode
    elif (work_branch is not None) & (not direct):
        local_branches = [branch.name for branch in repo.branches]
        if work_branch in local_branches:
            message = (
                f"Branch '{work_branch}' already exists locally."
                "Either choose a different name for the new branch to be created, "
                "or switch to the new branch and run this tool without specifying a new branch."
            )
            raise click.ClickException(message)
    return work_branch


def check_branches_valid(base_branch, work_branch, remote_branches):
    """Ensure the base branch exists in remote (this should always be true for 'master')."""
    # Check base branch (main)
    if base_branch not in remote_branches:
        raise click.ClickException(
            f"Base branch '{base_branch}' does not exist in remote. "
            "Either push that branch (git push origin base-branch-name) or use 'master' as a base branch. "
            "Then run this tool again."
        )
    # Check work branch
    if work_branch in remote_branches:
        raise click.ClickException(
            f"New branch '{work_branch}' already exists in remote. "
            "Either manually create a pull request from github, or use a different name for the new branch."
        )


def branch_out(repo, base_branch, work_branch):
    """Branch out from base_branch and create branch 'work_branch'."""
    try:
        log.info(
            f"Switching to base branch '{base_branch}', creating new branch '{work_branch}' from there, and switching to it."
        )
        repo.git.checkout(base_branch)
        repo.git.checkout("-b", work_branch)
    except GitCommandError as e:
        raise click.ClickException(f"Failed to create a new branch from '{base_branch}':\n{e}")


def create_pr(repo, work_branch, base_branch, pr_title):
    """Create a draft pull request work_branch -> base_branch."""
    pr_title_str = str(pr_title)

    log.info("Creating an empty commit.")
    repo.git.commit("--allow-empty", "-m", pr_title_str or f"Start a new staging server for branch '{work_branch}'")

    log.info("Pushing the new branch to remote.")
    repo.git.push("origin", work_branch)

    log.info("Creating a draft pull request.")
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    data = {
        "title": pr_title_str or f":construction: Draft PR for branch {work_branch}",
        "head": work_branch,
        "base": base_branch,
        "body": "",
        "draft": True,
    }
    response = requests.post("https://api.github.com/repos/owid/etl/pulls", json=data, headers=headers)
    if response.status_code == 201:
        js = response.json()
        log.info(f"Draft pull request created successfully at {js['html_url']}.")
    else:
        raise click.ClickException(f"Failed to create draft pull request:\n{response.json()}")


def _generate_pr_title(title: str, category: str, scope: str | None) -> Optional[str]:
    """Generate the PR title.

    title + category + scope -> 'category scope: title'
    title + category -> 'category title'
    """
    if title is not None:
        prefix = ""
        # Add emoji for PR mode chosen if applicable
        if category in PR_CATEGORIES:
            prefix = PR_CATEGORIES[category]["emoji"]
        else:
            raise ValueError(f"Invalid PR type '{category}'. Choose one of {list(PR_CATEGORIES.keys())}.")
        # Add scope
        if scope is not None:
            if prefix != "":
                prefix += " "
            prefix += f"{scope}:"

        # Add prefix
        title = f"{prefix} {title}"
    return title


def bake_branch_name(repo, pr_title, no_llm, remote_branches):
    # Get user
    # git_config = repo.config_reader()
    # user = git_config.get_value("user", "name").lower()

    # Get category
    category = pr_title.category

    # Get input title (without emoji, scope, etc.)
    title = _extract_relevant_title_for_branch_name(pr_title.title, category, not no_llm)

    # Bake complete PR branch name
    # name = f"{user}-{category}-{title}"
    name = f"{category}-{title}"

    # If branch name collision
    # if name in remote_branches:
    #     log.info("Generating a hash for this branch name to prevent name collisions.")
    #     name = f"{name}-{user}"
    local_branches = [branch.name for branch in repo.branches]
    if (name in remote_branches) or (name in local_branches):
        log.info("Generating a hash for this branch name to prevent name collisions.")
        name = f"{name}-{generate_short_hash()}"
    return name


def _extract_relevant_title_for_branch_name(text_in: str, category: str, use_llm) -> str:
    """
    Process the input string by:
    1. Removing all symbols, keeping only letters and numbers.
    2. Splitting into a list of words/tokens.
    3. Keeping only the first three tokens (or fewer if not available).
    4. Combining the tokens with a '-'.

    Args:
        text_in (str): The input text string.

    Returns:
        str: The processed string.
    """
    if use_llm:
        if "OPENAI_API_KEY" in os.environ:
            text_in = summarize_title_llm(text_in)

    cleaned_text = re.sub(r"[^a-zA-Z0-9\s]", "", text_in)

    # Split into tokens/words
    tokens = cleaned_text.split()

    # Clean if there is word included in category
    tokens = [t for t in tokens if t.lower() != category]

    # Keep only the first 3 tokens
    tokens = tokens[:3]

    # Combine tokens with '-'
    name = "-".join(tokens).lower()

    return name


def generate_short_hash() -> str:
    """
    Generate a random short hash (6 characters) using SHA256.

    Returns:
        str: A 6-character random hash string.
    """
    random_data = uuid.uuid4().hex  # Generate random data
    random_hash = hashlib.sha256(random_data.encode()).hexdigest()  # Create hash
    return random_hash[:6]  # Return the first 6 characters


def summarize_title_llm(title) -> str:
    sys_prompt = "You are given a title of a pull request. I need a 2-3 keyword summary, separated by a space. These words will be used to create a branch name."
    api = OpenAIWrapper()
    log.info("Querying GPT!")
    response = api.query_gpt_fast(title, sys_prompt, model="gpt-4o-mini")
    return response

"""This script creates a new draft pull request in GitHub, which starts a new staging server.

The new branch is seeded with an empty commit (enough to open the draft PR and start the
staging server). This commit is always empty: it never stages or commits your local changes,
whether or not you have anything staged.

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

**Custom use case (4)**: Create the new branch in a sibling git worktree so you can keep editing your current branch in parallel.

```shell
etl pr "some title for the PR" --worktree
# Shorter
etl pr "some title for the PR" -t
# With a custom path
etl pr "some title for the PR" -t --worktree-path /tmp/etl-mybranch
```

The new working directory is printed at the end (default: `../etl-BRANCH`); `cd` into it to start working there.

**Custom use case (5)**: Share the original repo's `data/`, `workbench/`, and `ai/` directories with the new worktree, so ETL steps don't have to recompute population, regions, etc., and Claude Code skills (e.g. `update-dataset`) write their scratch artifacts straight into the main repo.

```shell
etl pr "some title for the PR" -t --share-data
```

This makes the new worktree's `data/`, `workbench/`, and `ai/` shortcuts (symlinks) to the original repo's, so both worktrees share the same outputs and you don't have to recompute or salvage them. Since the real content lives in the main repo, removing the worktree — by any method, including a plain `git worktree remove` — never touches it; only the symlink itself is deleted. Note:
- If you run the same steps in both worktrees, they may overwrite each other's output.
- DO NOT use `rm -rf data/` / `rm -rf workbench/` / `rm -rf ai/` (with the trailing slash) — that follows the symlink and wipes the original folder along with the worktree's copy. Instead, use `etl pr-clean` (or `git worktree remove ../etl-[whatever-branch]`) to remove a worktree; both remove only the symlinks.

After the command finishes, `uv sync` has already run inside the worktree, so its `.venv/` is ready to use. With a `chpwd` hook in your `~/.zshrc` that sources `.venv/bin/activate` whenever present, `cd ../etl-BRANCH` is all that's needed — activation is automatic. Without the hook, also run `source .venv/bin/activate` after the cd. Skipping activation silently routes `etl`/`etlr` to the original repo's source code.

See the docs (`Working on multiple branches in parallel`) for full details and tips.
"""

import hashlib
import os
import re
import shutil
import subprocess
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import cast

import click
import questionary
import requests
from git import GitCommandError, InvalidGitRepositoryError, Repo
from rich_click.rich_command import RichCommand
from structlog import get_logger

from apps.pr.categories import PR_CATEGORIES, PR_CATEGORIES_CHOICES
from apps.utils.llms.gpt import OpenAIWrapper
from etl.config import GITHUB_API_BASE, GITHUB_API_URL, GITHUB_TOKEN
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

# LLM
MODEL_DEFAULT = "gpt-5-mini"


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
@click.option(
    "--worktree",
    "-t",
    is_flag=True,
    help="Create the new branch in a sibling git worktree (default: ../etl-BRANCH) instead of mutating the current working tree. Useful for working on multiple branches in parallel.",
)
@click.option(
    "--worktree-path",
    "worktree_path",
    type=str,
    default=None,
    help="Override the worktree directory (only with --worktree). Defaults to ../etl-BRANCH.",
)
@click.option(
    "--share-data",
    "share_data",
    is_flag=True,
    help="Symlink the new worktree's data/, workbench/, and ai/ to the original repo's (only with --worktree). Avoids recomputing upstream ETL steps and losing workbench/ai scratch artifacts on worktree removal. Don't run heavy ETL ops in both worktrees concurrently, and never `rm -rf data/` (or workbench/ or ai/) in the worktree.",
)
@click.option(
    "--auto-assign",
    is_flag=True,
    help="Automatically assign the PR to the GitHub user the token belongs to.",
)
def cli(
    title: str,
    category: str | None,
    scope: str | None,
    work_branch: str | None,
    base_branch: str,
    direct: bool,
    private: bool,
    no_llm: bool,
    worktree: bool,
    worktree_path: str | None,
    share_data: bool,
    auto_assign: bool,
    # base_branch: Optional[str] = None,
) -> None:
    # Check that the user has set up a GitHub token.
    check_gh_token()

    # Validate title
    _validate_title(title)

    # --worktree and --direct don't compose: --direct reuses the current branch in
    # the current working tree, --worktree creates a new isolated working tree.
    if worktree and direct:
        raise click.ClickException(
            "--worktree and --direct cannot be used together. "
            "--direct reuses the current branch in the current working tree, "
            "while --worktree creates a brand-new isolated working tree."
        )
    if worktree_path and not worktree:
        raise click.ClickException("--worktree-path requires --worktree.")
    if share_data and not worktree:
        raise click.ClickException("--share-data requires --worktree.")

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

    resolved_worktree_path: Path | None = None

    # Auto PR mode: Create a new branch from the base branch.
    if not direct:
        if private:
            if not work_branch.endswith("-private"):
                work_branch = f"{work_branch}-private"
        if worktree:
            resolved_worktree_path = resolve_worktree_path(work_branch, worktree_path)
            branch_out_worktree(repo, base_branch, work_branch, resolved_worktree_path)
            if share_data:
                symlink_shared_dirs(resolved_worktree_path)
            # Subsequent git operations (commit, push) must run inside the worktree.
            repo = Repo(resolved_worktree_path)
        else:
            branch_out(repo, base_branch, work_branch)

    # Create PR
    create_pr(repo, work_branch, base_branch, pr_title, auto_assign=auto_assign)

    if resolved_worktree_path is not None:
        venv_ok = install_worktree_venv(resolved_worktree_path)
        print_worktree_hint(
            resolved_worktree_path,
            work_branch=work_branch,
            shared_data=share_data,
            venv_installed=venv_ok,
        )


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


def ensure_category(category: str | None):
    """Get category if not provided."""
    if category is None:
        # show suggestions
        choices = [questionary.Choice(**choice) for choice in PR_CATEGORIES_CHOICES]  # ty: ignore
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


def resolve_worktree_path(work_branch: str, override: str | None) -> Path:
    """Resolve the absolute path where the new worktree should be created."""
    if override:
        return Path(override).expanduser().resolve()
    return (BASE_DIR.parent / f"etl-{work_branch}").resolve()


def branch_out_worktree(repo, base_branch: str, work_branch: str, worktree_path: Path) -> None:
    """Create branch 'work_branch' from 'base_branch' inside a new git worktree at 'worktree_path'."""
    if worktree_path.exists():
        raise click.ClickException(
            f"Worktree path '{worktree_path}' already exists. "
            "Choose a different --worktree-path or remove the existing directory."
        )
    try:
        log.info(f"Creating worktree at '{worktree_path}' with new branch '{work_branch}' from '{base_branch}'.")
        repo.git.worktree("add", "-b", work_branch, str(worktree_path), base_branch)
    except GitCommandError as e:
        raise click.ClickException(f"Failed to create worktree at '{worktree_path}':\n{e}")

    # Copy .env so the new worktree is immediately usable. .venv is intentionally not
    # copied/symlinked — it's Python-version-specific and best recreated with `make check`.
    src_env = BASE_DIR / ".env"
    if src_env.exists():
        shutil.copy2(src_env, worktree_path / ".env")
        log.info(f"Copied .env to '{worktree_path / '.env'}'.")
    else:
        log.debug(f"No .env found at '{src_env}', skipping copy.")


# Gitignored dirs symlinked into the worktree by --share-data.
SHARED_SCRATCH_DIRS = ("workbench", "ai")


def symlink_shared_dirs(worktree_path: Path) -> None:
    """Symlink data/, workbench/, and ai/ from the original repo into the new worktree.

    data/ lets ETL steps reuse cached outputs instead of recomputing them. workbench/ and ai/ are
    gitignored scratch dirs that Claude Code skills (e.g. update-dataset) write progress logs and
    notes into — sharing them means that content lives in the main repo from the start, so removing
    the worktree by any method never risks losing it (unlike relying on `pr-clean`'s post-hoc copy).
    """
    src = BASE_DIR / "data"
    dst = worktree_path / "data"
    if not src.exists():
        log.warning(f"Cannot share data: '{src}' does not exist in the original repo.")
    elif dst.exists() or dst.is_symlink():
        # `git worktree add` shouldn't have created a `data/` (it's gitignored), but be defensive.
        log.warning(f"'{dst}' already exists, skipping data symlink.")
    else:
        os.symlink(src, dst)
        log.info(f"Symlinked '{dst}' -> '{src}'.")

    for name in SHARED_SCRATCH_DIRS:
        src = BASE_DIR / name
        dst = worktree_path / name
        if dst.exists() or dst.is_symlink():
            log.warning(f"'{dst}' already exists, skipping {name} symlink.")
            continue
        src.mkdir(parents=True, exist_ok=True)  # gitignored scratch dir; fine to create if missing
        os.symlink(src, dst)
        log.info(f"Symlinked '{dst}' -> '{src}'.")


def install_worktree_venv(worktree_path: Path) -> bool:
    """Run `uv sync` in the new worktree to set up its `.venv/`. Returns True on success.

    This matches what the project's Makefile does for the `.venv` target — see
    `default.mk` (`uv sync --all-extras --group dev`). Doing this automatically lets
    the user just `cd` into the new worktree afterwards (with a chpwd hook the venv
    activates automatically; without one, only `source .venv/bin/activate` is needed).
    """
    log.info(f"Installing dependencies in '{worktree_path}' (one-time, ~1 min)...")
    try:
        subprocess.run(
            ["uv", "sync", "--all-extras", "--group", "dev"],
            cwd=worktree_path,
            check=True,
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, KeyboardInterrupt) as e:
        log.warning(
            f"Could not set up the venv automatically ({e}). You'll need to run `make check` inside the worktree."
        )
        return False


def print_worktree_hint(
    worktree_path: Path,
    work_branch: str,
    shared_data: bool = False,
    venv_installed: bool = False,
) -> None:
    """Tell the user how to land in the new worktree.

    A child Python process can't change its parent shell's cwd, so we just print a
    ready-to-paste cd line.
    """
    # Prefer a relative path from cwd if it's shorter than the absolute path
    # (e.g. `../etl-foo` is friendlier than the full /Users/.../etl-foo).
    rel_path = os.path.relpath(worktree_path)
    display_path = rel_path if len(rel_path) < len(str(worktree_path)) else str(worktree_path)

    print()
    print(f"Worktree ready at: {worktree_path}")
    print()
    if venv_installed:
        print("To start working there, just run:")
        print(f"  cd {display_path}")
        print()
        print("If you have a chpwd hook in ~/.zshrc, the venv activates automatically.")
        print("Otherwise also run: source .venv/bin/activate")
    else:
        print("Auto venv setup didn't complete. Inside the worktree, run:")
        print(f"  cd {display_path}")
        print("  make check")
        print("  source .venv/bin/activate")
    print()
    print("When you're done (PR merged or closed), clean up worktrees and branches with:")
    print("  etl pr-clean")
    print("It removes the worktree, deletes the branch, and copies this worktree's Claude")
    print("sessions back into the main repo so they're still resumable from there.")
    if shared_data:
        print()
        print("WARNING: data/, workbench/, and ai/ are symlinks to the original repo's, so:")
        print("  - If you run the same steps in both worktrees, they may overwrite each other's output.")
        print("  - DO NOT use `rm -rf data/` (or workbench/ or ai/); this follows the symlink and wipes")
        print("    the original folder along with the worktree's copy.")
        print(
            "    Instead, use `etl pr-clean` (or `git worktree remove ../etl-[whatever-branch]`) to remove a worktree —"
        )
        print("    both remove only the symlinks, leaving the shared content untouched.")


def create_pr(repo, work_branch, base_branch, pr_title, auto_assign: bool = False):
    """Create a draft pull request work_branch -> base_branch."""
    pr_title_str = str(pr_title)

    log.info("Creating an empty commit.")
    # Build the seeding commit with `git commit-tree` rather than `git commit`. It reuses
    # HEAD's own tree, so the commit is always truly empty: it never reads the index, hence
    # it can't accidentally commit changes you'd already staged, and it runs no pre-commit
    # hooks (so it won't fail on formatting/typing, and needs no `.venv` in a fresh worktree).
    head = repo.head.commit
    commit_msg = pr_title_str or f"Start a new staging server for branch '{work_branch}'"
    new_commit = repo.git.commit_tree(head.tree.hexsha, "-p", head.hexsha, "-m", commit_msg)
    repo.git.update_ref("HEAD", new_commit)

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
    assignee: str | None = None
    if auto_assign:
        user_response = requests.get("https://api.github.com/user", headers=headers)
        if user_response.status_code == 200:
            assignee = user_response.json()["login"]
        else:
            log.warning(f"Could not fetch GitHub user for --auto-assign (HTTP {user_response.status_code}), skipping.")
    response = requests.post(GITHUB_API_URL, json=data, headers=headers)
    if response.status_code == 201:
        js = response.json()
        log.info(f"Draft pull request created successfully at {js['html_url']}.")
        if assignee:
            pr_number = js["number"]
            assign_url = f"{GITHUB_API_BASE}/issues/{pr_number}/assignees"
            assign_response = requests.post(assign_url, json={"assignees": [assignee]}, headers=headers)
            if assign_response.status_code == 201:
                log.info(f"Assigned PR to {assignee}.")
            else:
                log.warning(f"Could not assign PR to {assignee} (HTTP {assign_response.status_code}).")
    else:
        raise click.ClickException(f"Failed to create draft pull request:\n{response.json()}")


def _generate_pr_title(title: str, category: str, scope: str | None) -> str | None:
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
    response = api.query_gpt_fast(title, sys_prompt, model=MODEL_DEFAULT)
    return response


# ----------------------------------------------------------------------------------------------------------------------
# etl pr-clean
# ----------------------------------------------------------------------------------------------------------------------

CLEAN_HELP = """Clean up local branches whose pull request was merged or closed on GitHub.

Lists every local branch whose latest GitHub PR is **merged** or **closed** (using GitHub's PR
state, not `git branch --merged`, so squash-merges are detected). Branches that have a git worktree
are flagged with `<- worktree`. Pick a single branch, or `all`, and the tool will:

1. (Worktree branches only) Copy that worktree's Claude sessions — the `<uuid>.jsonl` transcripts and
   their `<uuid>/` subfolders under `~/.claude/projects/<encoded-worktree-path>/` — into the main
   repo's project dir, so they stay resumable with `claude --resume` after the worktree is gone.
2. (Worktree branches only) Copy the worktree's gitignored `workbench/` and `ai/` scratch dirs into
   `workbench/<branch>/` and `ai/<branch>/` in the main repo (suffixed `-1`, `-2`... on the rare name
   clash), so the working notes/outputs survive the worktree removal without overwriting anything.
   Skipped for a dir created by `--share-data` (it's a symlink to the main repo already — nothing to
   salvage).
3. Remove the git worktree (skipped with a warning if it has uncommitted changes). `--share-data`'s
   symlinks are unlinked first so they don't themselves block the removal.
4. Delete the local branch.

Each branch is tagged `[merged]` or `[closed]` so you can see its PR outcome before selecting.

Must be run from the main repo, not from a secondary worktree (it errors out otherwise). The main
repo can clean every worktree anyway, so just `cd` there first.

```shell
etl pr-clean
```
"""


@click.command(
    name="pr-clean",
    cls=RichCommand,
    help=CLEAN_HELP,
)
def clean_cli() -> None:
    check_gh_token()

    repo = Repo(BASE_DIR)
    main_worktree_path = get_main_worktree_path(repo)
    worktrees = list_worktrees(repo)  # branch name -> worktree path

    # Resolve the worktree the user is actually standing in from cwd, not from Repo(BASE_DIR).
    # BASE_DIR is the package location, so when etl runs from the main repo's venv while the shell
    # is inside a secondary worktree (that worktree's venv not activated), Repo(BASE_DIR) still
    # points at the main repo — comparing its working_tree_dir would wrongly pass the guard below.
    try:
        cwd_repo = Repo(Path.cwd(), search_parent_directories=True)
        current_worktree_path = Path(cwd_repo.working_tree_dir).resolve()  # ty: ignore
    except InvalidGitRepositoryError:
        # cwd isn't inside any git repo — keep the raw cwd so the guard still errors out (it can't
        # equal the main worktree), rather than falling back to BASE_DIR's repo and passing.
        current_worktree_path = Path.cwd().resolve()

    # pr-clean must run from the main repo, never from a secondary worktree. From a worktree it
    # could try to remove the working tree you're standing in, or — if that worktree was switched
    # off its branch (e.g. to master) — delete the branch while leaving the now-unlinked worktree
    # orphaned. The main repo can clean every worktree anyway, so just bail and point there.
    if current_worktree_path != main_worktree_path:
        raise click.ClickException(
            "pr-clean must be run from the main repo, not a worktree.\n"
            f"  You're in:     {current_worktree_path}\n"
            f"  cd to main and re-run:\n    cd {main_worktree_path}"
        )

    # Candidate branches: every local branch except master.
    candidate_branches = [b.name for b in repo.branches if b.name != "master"]
    if not candidate_branches:
        print("No local branches to clean (only 'master' found).")
        return

    # Resolve each branch's PR state from GitHub (parallelised — one API call per branch).
    log.info(f"Checking GitHub PR state for {len(candidate_branches)} local branch(es)...")
    with ThreadPoolExecutor(max_workers=8) as executor:
        states = dict(zip(candidate_branches, executor.map(fetch_pr_state, candidate_branches)))

    # Keep only branches whose PR was merged or closed; active (open / no-PR) branches are left alone.
    cleanable = {b: s for b, s in states.items() if s in ("merged", "closed")}
    if not cleanable:
        print("Nothing to clean: no local branch has a merged or closed PR.")
        return

    # Build the selection list ('all' first, then one entry per branch).
    choices = [questionary.Choice(title="all", value="__all__")]
    for branch in sorted(cleanable):
        choices.append(questionary.Choice(title=_branch_label(branch, cleanable[branch], worktrees), value=branch))

    selected = questionary.select(
        message="Select a branch to clean (or 'all')",
        choices=choices,
        style=SHELL_FORM_STYLE,
        instruction="(Use arrow keys, Enter to confirm)",
    ).unsafe_ask()

    if selected is None:
        return

    branches_to_clean = sorted(cleanable) if selected == "__all__" else [selected]

    main_project_dir = claude_project_dir(main_worktree_path)

    for branch in branches_to_clean:
        clean_branch(
            repo=repo,
            branch=branch,
            worktree_path=worktrees.get(branch),
            main_project_dir=main_project_dir,
            main_worktree_path=main_worktree_path,
        )


def get_main_worktree_path(repo) -> Path:
    """Return the path of the primary (main) worktree, the first entry of `git worktree list`."""
    porcelain = repo.git.worktree("list", "--porcelain")
    for line in porcelain.splitlines():
        if line.startswith("worktree "):
            return Path(line[len("worktree ") :]).resolve()
    # Fallback: the repo we were initialised from.
    return Path(repo.working_tree_dir).resolve()


def list_worktrees(repo) -> dict[str, Path]:
    """Map each checked-out branch name to its worktree path, parsing `git worktree list --porcelain`."""
    porcelain = repo.git.worktree("list", "--porcelain")
    result: dict[str, Path] = {}
    current_path: Path | None = None
    for line in porcelain.splitlines():
        if line.startswith("worktree "):
            current_path = Path(line[len("worktree ") :]).resolve()
        elif line.startswith("branch ") and current_path is not None:
            # e.g. "branch refs/heads/data-foo" -> "data-foo".
            branch = line[len("branch ") :].removeprefix("refs/heads/")
            result[branch] = current_path
    return result


def fetch_pr_state(branch: str) -> str | None:
    """Return the GitHub PR state for a branch: 'open', 'merged', 'closed', or None.

    None means either no PR exists for the branch or the GitHub request failed (logged as a warning);
    in both cases the branch is treated as not-cleanable and left alone. Uses GitHub's PR state rather
    than git ancestry so squash-merged PRs are correctly seen as merged.
    """
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    params = {"state": "all", "head": f"owid:{branch}", "per_page": 50}
    try:
        response = requests.get(f"{GITHUB_API_BASE}/pulls", params=params, headers=headers, timeout=30)
    except requests.RequestException as e:
        log.warning(f"Could not fetch PR state for '{branch}': {e}")
        return None
    if response.status_code != 200:
        log.warning(f"Could not fetch PR state for '{branch}' (HTTP {response.status_code}).")
        return None
    prs = response.json()
    if not prs:
        return None
    # An open PR means the branch is still active. A merged PR (state 'closed' with merged_at set)
    # wins over a plain closed one. Otherwise the branch's PRs were closed without merging.
    if any(pr.get("state") == "open" for pr in prs):
        return "open"
    if any(pr.get("merged_at") for pr in prs):
        return "merged"
    return "closed"


def _branch_label(branch: str, state: str, worktrees: dict[str, Path]) -> str:
    """Build the questionary label for a branch, e.g. 'data-foo  [merged]  <- worktree'."""
    label = f"{branch}  [{state}]"
    if branch in worktrees:
        label += "  <- worktree"
    return label


def claude_project_dir(repo_path: Path) -> Path:
    """Return the `~/.claude/projects/<encoded-path>` dir where Claude stores sessions for a repo path.

    Claude encodes the absolute path by replacing every non-alphanumeric character with '-', so
    '/Users/me/repos/etl-foo' becomes '-Users-me-repos-etl-foo'.
    """
    encoded = re.sub(r"[^a-zA-Z0-9]", "-", str(repo_path))
    return Path.home() / ".claude" / "projects" / encoded


def copy_sessions(worktree_path: Path, main_project_dir: Path) -> int:
    """Copy a worktree's Claude sessions into the main repo's project dir. Returns the number of items copied.

    Session UUIDs are globally unique, so this never collides with the main repo's own sessions. Anything
    that already exists at the destination (file or dir) is skipped, so re-runs are idempotent.
    """
    src = claude_project_dir(worktree_path)
    if not src.exists():
        log.info(f"No Claude sessions found at '{src}', nothing to copy.")
        return 0

    main_project_dir.mkdir(parents=True, exist_ok=True)
    copied = 0
    for item in src.iterdir():
        if item.name == ".DS_Store":
            continue
        dest = main_project_dir / item.name
        if dest.exists():
            continue
        if item.is_dir():
            shutil.copytree(item, dest)
        else:
            shutil.copy2(item, dest)
        copied += 1
    log.info(f"Copied {copied} session item(s) from '{src}' to '{main_project_dir}'.")
    return copied


def copy_dir_namespaced(src: Path, dst_parent: Path, branch: str) -> Path | None:
    """Copy a worktree's `src` dir into `dst_parent/<branch>`, suffixing -1, -2... on collision.

    Returns the destination path, or None if `src` is missing, not a directory, or empty. Unlike
    Claude sessions (UUID-named, collision-free), these dirs use task/human names, so the whole tree
    lands under a branch-named (and, on collision, suffixed) folder. Slashes in the branch name are
    flattened to '-' so the result stays a single folder. Nothing already in `dst_parent` is overwritten.
    """
    if not src.is_dir() or not any(p.name != ".DS_Store" for p in src.iterdir()):
        return None

    safe_branch = branch.replace("/", "-")
    dest = dst_parent / safe_branch
    i = 1
    while dest.exists():
        dest = dst_parent / f"{safe_branch}-{i}"
        i += 1

    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src, dest, ignore=shutil.ignore_patterns(".DS_Store"))
    log.info(f"Copied '{src}' to '{dest}'.")
    return dest


def clean_branch(
    repo,
    branch: str,
    worktree_path: Path | None,
    main_project_dir: Path,
    main_worktree_path: Path,
) -> None:
    """Copy sessions (if a worktree), remove the worktree, and delete the local branch."""
    # The branch checked out in the main repo (where pr-clean runs) can't be cleaned from here.
    if worktree_path is not None and worktree_path == main_worktree_path:
        log.warning(
            f"Skipping '{branch}': it's checked out in the main repo. Switch the main repo to another branch first."
        )
        return

    if worktree_path is not None:
        # Salvage anything the worktree holds but that's gitignored (so `git worktree remove` would
        # destroy it for good): the Claude sessions, plus the workbench/ and ai/ scratch dirs.
        # The session dir lives outside the worktree, but copy everything before removal on principle.
        copy_sessions(worktree_path, main_project_dir)
        for name in SHARED_SCRATCH_DIRS:
            src = worktree_path / name
            if src.is_symlink():
                # Created by --share-data: the real content already lives in the main repo, so
                # there's nothing to salvage — copying would just duplicate it wholesale.
                continue
            copy_dir_namespaced(src, main_worktree_path / name, branch)

        # `git worktree remove` refuses a worktree with untracked files, and --share-data's
        # symlinks (data/, workbench/, ai/) count as untracked. Unlink them ourselves first — safe,
        # since their real content lives in the main repo, untouched — so only genuine leftover
        # uncommitted/untracked work still blocks the plain removal below.
        for name in ("data", *SHARED_SCRATCH_DIRS):
            link = worktree_path / name
            if link.is_symlink():
                link.unlink()

        try:
            repo.git.worktree("remove", str(worktree_path))
            log.info(f"Removed worktree '{worktree_path}'.")
        except GitCommandError as e:
            log.warning(
                f"Could not remove worktree '{worktree_path}' (uncommitted changes?). "
                f"Leaving branch '{branch}' in place.\n{e}"
            )
            return

    try:
        repo.git.branch("-D", branch)
        log.info(f"Deleted local branch '{branch}'.")
    except GitCommandError as e:
        log.warning(f"Could not delete branch '{branch}':\n{e}")

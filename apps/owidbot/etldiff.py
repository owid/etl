import datetime as dt
import subprocess
import time
from typing import Tuple

import click
import structlog
from github import Auth, Github
from rich import print
from rich.ansi import AnsiDecoder
from rich_click.rich_command import RichCommand

from apps.staging_sync.cli import _get_container_name
from etl import config
from etl.paths import BASE_DIR

log = structlog.get_logger()


EXCLUDE_DATASETS = "weekly_wildfires|excess_mortality|covid|fluid|flunet|country_profile"


@click.command(name="owidbot-etl-diff", cls=RichCommand, help=__doc__)
@click.option(
    "--branch",
    type=str,
)
@click.option(
    "--include",
    type=str,
    default="garden",
    help="Include datasets matching this regex.",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    type=bool,
    help="Print to console, do not post to Github.",
)
def cli(
    branch: str,
    include: str,
    dry_run: bool,
) -> None:
    """Post result of `etl diff` to Github PR.

    Example:

    ```
    $ python apps/owidbot/etldiff.py --branch my-branch
    ```
    """
    t = time.time()

    lines = call_etl_diff(include)
    diff, result = format_etl_diff(lines)

    container_name = _get_container_name(branch) if branch else "dry-run"

    # TODO: only include site-screenshots if the PR is from owid-grapher. Similarly, don't
    # run etl diff if the PR is from etl repo.
    # - **Site-screenshots**: https://github.com/owid/site-screenshots/compare/{nbranch}

    body = f"""
<details>

<summary><b>Staging server</b>: </summary>

- **Admin**: http://{container_name}/admin/login
- **Site**: http://{container_name}/
- **Login**: `ssh owid@{container_name}`
</details>

<details>

<summary><b>etl diff</b>: {result}</summary>

```diff
{diff}
```

Automatically updated datasets matching _{EXCLUDE_DATASETS}_ are not included
</details>

_Edited: {dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")} UTC_
_Execution time: {time.time() - t:.2f} seconds_
    """.strip()

    if dry_run:
        print(body)
    else:
        post_comment_to_pr(branch, body)


def post_comment_to_pr(branch_name: str, body: str) -> None:
    assert config.OWIDBOT_ACCESS_TOKEN
    auth = Auth.Token(config.OWIDBOT_ACCESS_TOKEN)
    g = Github(auth=auth)

    repo = g.get_repo("owid/etl")

    # Find pull requests for the branch (assuming you're looking for open PRs)
    pulls = repo.get_pulls(state="open", sort="created", head=f"{repo.owner.login}:{branch_name}")
    pulls = list(pulls)

    if len(pulls) == 0:
        raise AssertionError(f"No open PR found for branch {branch_name}")
    elif len(pulls) > 1:
        raise AssertionError(f"More than one open PR found for branch {branch_name}")

    pr = pulls[0]

    comments = pr.get_issue_comments()

    owidbot_comments = [comment for comment in comments if comment.user.login == "owidbot"]

    if len(owidbot_comments) == 0:
        pr.create_issue_comment(body=body)
    elif len(owidbot_comments) == 1:
        owidbot_comment = owidbot_comments[0]
        owidbot_comment.edit(body=body)
    else:
        raise AssertionError("More than one owidbot comment found.")


def format_etl_diff(lines: list[str]) -> Tuple[str, str]:
    new_lines = []
    result = ""
    for line in lines:
        # extract result
        if line and line[0] in ("✅", "❌", "⚠️", "❓"):
            result = line
            continue

        # skip some lines
        if "this may get slow" in line or "comparison with compare" in line:
            continue

        if line.strip().startswith("-"):
            line = "-" + line[1:]
        if line.strip().startswith("+"):
            line = "+" + line[1:]

        new_lines.append(line)

    diff = "\n".join(new_lines)

    # NOTE: we don't need this anymore, we now have consistent checksums on local and remote
    # Some datasets might have different checksum, but be the same (this is caused by checksum_input and checksum_output
    # problem). Hotfix this by removing matching datasets from the output.
    # Example:
    # = Dataset meadow/agriculture/2024-03-26/attainable_yields
    #     = Table attainable_yields
    # = Dataset garden/agriculture/2024-03-26/attainable_yields
    #     = Table attainable_yields
    #        ~ Column A
    # = Dataset grapher/agriculture/2024-03-26/attainable_yields
    #     = Table attainable_yields
    # pattern = r"(= Dataset.*(?:\n\s+=.*)+)\n(?=. Dataset|\n)"
    # diff = re.sub(pattern, "", diff)

    # Github has limit of 65,536 characters
    if len(diff) > 64000:
        diff = diff[:64000] + "\n\n...diff too long, truncated..."

    return diff, result


def call_etl_diff(include: str) -> list[str]:
    cmd = [
        "poetry",
        "run",
        "etl",
        "diff",
        "REMOTE",
        "data/",
        "--include",
        include,
        "--exclude",
        EXCLUDE_DATASETS,
        "--verbose",
        "--workers",
        "3",
    ]

    result = subprocess.Popen(cmd, cwd=BASE_DIR, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = result.communicate()

    stdout = stdout.decode()
    stderr = stderr.decode()

    if stderr:
        raise Exception(f"Error: {stderr}")

    return [str(line) for line in AnsiDecoder().decode(stdout)]


if __name__ == "__main__":
    cli()

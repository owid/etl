import hashlib
import time
from typing import Any, Optional

import github
import github.PullRequest
import github.Repository
import jwt
import requests
import structlog
from github import Auth, Github

from etl import config

log = structlog.get_logger()


def get_repo(repo_name: str, access_token: Optional[str] = None) -> github.Repository.Repository:
    if not access_token:
        # Don't auth, be aware that you won't be able to do write operations. You should
        # set up your access token on https://github.com/settings/tokens.
        auth = None
    else:
        auth = Auth.Token(access_token)

    g = Github(auth=auth)
    return g.get_repo(f"owid/{repo_name}")


def get_pr(repo: github.Repository.Repository, branch_name: str) -> Optional[github.PullRequest.PullRequest]:
    # Find pull requests for the branch (assuming you're looking for open PRs)
    pulls = repo.get_pulls(state="open", sort="created", head=f"{repo.owner.login}:{branch_name}")
    pulls = list(pulls)

    if len(pulls) == 0:
        return None
    elif len(pulls) > 1:
        log.warning(f"More than one open PR found for branch {branch_name}. Taking the most recent one.")
        pulls = pulls[-1:]

    return pulls[0]


def get_comment_from_pr(pr: Any) -> Optional[Any]:
    comments = pr.get_issue_comments()

    owidbot_comments = [comment for comment in comments if comment.user.login == "owidbot"]

    if len(owidbot_comments) == 0:
        return None
    elif len(owidbot_comments) == 1:
        return owidbot_comments[0]
    else:
        raise AssertionError("More than one owidbot comment found.")


def generate_jwt(client_id: str, private_key_path: str) -> str:
    now = int(time.time())
    payload = {
        "iat": now,
        "exp": now + (10 * 60),  # JWT expiration time (10 minutes)
        "iss": client_id,
    }
    with open(private_key_path, "r") as key_file:
        private_key = key_file.read()
    token = jwt.encode(payload, private_key, algorithm="RS256")
    return token


def github_app_access_token(max_retries=3) -> str:
    assert config.OWIDBOT_APP_CLIENT_ID, "OWIDBOT_APP_CLIENT_ID is not set"
    assert config.OWIDBOT_APP_PRIVATE_KEY_PATH, "OWIDBOT_APP_PRIVATE_KEY_PATH is not set"
    assert config.OWIDBOT_APP_INSTALLATION_ID, "OWIDBOT_APP_INSTALLATION_ID is not set"

    jwt_token = generate_jwt(config.OWIDBOT_APP_CLIENT_ID, config.OWIDBOT_APP_PRIVATE_KEY_PATH)

    # Use the JWT to get an installation access token
    headers = {"Authorization": f"Bearer {jwt_token}", "Accept": "application/vnd.github.v3+json"}
    installation_access_token_url = (
        f"https://api.github.com/app/installations/{config.OWIDBOT_APP_INSTALLATION_ID}/access_tokens"
    )

    backoff = 2
    for attempt in range(1, max_retries + 1):
        response = requests.post(installation_access_token_url, headers=headers)
        if response.status_code not in (500, 504):
            response.raise_for_status()
            access_token = response.json()["token"]
            return access_token
        else:
            if attempt == max_retries:
                response.raise_for_status()
            else:
                time.sleep(backoff)
                backoff *= 2  # Exponential backoff

    raise AssertionError("Failed to get installation access token.")


def compute_git_blob_sha1(content: bytes) -> str:
    """Compute the SHA-1 hash of a file as Git would."""
    # Calculate the blob header
    size = len(content)
    header = f"blob {size}\0".encode("utf-8")

    # Compute the SHA-1 hash of the header + content
    sha1 = hashlib.sha1()
    sha1.update(header + content)
    return sha1.hexdigest()


def _github_access_token():
    # Use GITHUB_TOKEN if set, otherwise use OWIDBOT_ACCESS_TOKEN
    if config.GITHUB_TOKEN:
        return config.GITHUB_TOKEN
    elif config.OWIDBOT_ACCESS_TOKEN:
        return config.OWIDBOT_ACCESS_TOKEN
    else:
        raise AssertionError("You need to set GITHUB_TOKEN or OWIDBOT_ACCESS_TOKEN in your .env file to commit.")


def create_branch_if_not_exists(repo_name: str, branch: str, dry_run: bool) -> None:
    """Create a branch if it doesn't exist."""
    repo = get_repo(repo_name, access_token=_github_access_token())
    try:
        repo.get_branch(branch)
    except github.GithubException as e:
        if e.status == 404:
            if not dry_run:
                try:
                    master_ref = repo.get_branch("main").commit.sha
                    log.info(f"Using 'main' branch as reference for creating {branch}.")
                except github.GithubException:
                    master_ref = repo.get_branch("master").commit.sha
                    log.info(f"Using 'master' branch as reference for creating {branch}.")
                log.info(f"Creating branch {branch} with reference {master_ref}.")
                repo.create_git_ref(ref=f"refs/heads/{branch}", sha=master_ref)
            log.info(f"Branch {branch} created in {repo.name}.")
        else:
            raise e


def commit_file_to_github(
    content: str,
    repo_name: str,
    file_path: str,
    commit_message: str,
    branch: str,
    dry_run: bool = True,
) -> None:
    """Commit a table to a GitHub repository using the GitHub API."""
    # Get the repository object
    repo = get_repo(repo_name, access_token=_github_access_token())
    new_content_checksum = compute_git_blob_sha1(content.encode("utf-8"))

    try:
        # Check if the file already exists
        contents = repo.get_contents(file_path, ref=branch)

        # Compare the existing content with the new content
        if contents.sha == new_content_checksum:  # type: ignore
            log.info(
                f"File {file_path} is identical to the current version in {repo_name} on branch {branch}. No commit will be made."
            )
            return

        # Update the file
        if not dry_run:
            repo.update_file(contents.path, commit_message, content, contents.sha, branch=branch)  # type: ignore
    except Exception as e:
        # If the file doesn't exist, create a new file
        if "404" in str(e):
            if not dry_run:
                repo.create_file(file_path, commit_message, content, branch=branch)
        else:
            raise e

    if dry_run:
        log.info(f"Would have committed {file_path} to {repo_name} on branch {branch}.")
    else:
        log.info(f"Committed {file_path} to {repo_name} on branch {branch}.")


def get_git_branch_from_commit_sha(commit_sha: str) -> str:
    """Get the branch name from a merged pull request commit sha. This is useful for Buildkite jobs where we only have the commit sha."""
    if config.OWIDBOT_ACCESS_TOKEN:
        headers = {"Authorization": f"token {config.OWIDBOT_ACCESS_TOKEN}"}
    else:
        headers = {}

    # get all pull requests for the commit
    pull_requests = requests.get(
        f"https://api.github.com/repos/owid/etl/commits/{commit_sha}/pulls", headers=headers
    ).json()

    # filter the closed ones
    closed_pull_requests = [pr for pr in pull_requests if pr["state"] == "closed"]

    # get the branch of the most recent one
    if closed_pull_requests:
        return closed_pull_requests[0]["head"]["ref"]
    else:
        raise ValueError(f"No closed pull requests found for commit {commit_sha}")


def get_prs_from_repo(repo_name: str) -> list[dict]:
    # Start with the first page
    url = f"https://api.github.com/repos/owid/{repo_name}/pulls?per_page=100"
    if config.OWIDBOT_ACCESS_TOKEN:
        headers = {"Authorization": f"token {config.OWIDBOT_ACCESS_TOKEN}"}
    else:
        headers = {}

    active_prs = []
    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # To handle HTTP errors
        js = response.json()

        # Collect PR head refs from the current page
        for d in js:
            # only owid PRs
            if d["head"]["label"].startswith("owid:"):
                active_prs.append(d["head"]["ref"])

        # Check for the 'next' page link in the headers
        if "next" in response.links:
            url = response.links["next"]["url"]
        else:
            url = None  # No more pages

    # exclude dependabot PRs
    active_prs = [pr for pr in active_prs if "dependabot" not in pr]

    return active_prs

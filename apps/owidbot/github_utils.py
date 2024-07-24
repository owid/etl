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
        assert config.OWIDBOT_ACCESS_TOKEN, "OWIDBOT_ACCESS_TOKEN is not set"
        access_token = config.OWIDBOT_ACCESS_TOKEN
    auth = Auth.Token(access_token)
    g = Github(auth=auth)
    return g.get_repo(f"owid/{repo_name}")


def get_pr(repo: github.Repository.Repository, branch_name: str) -> github.PullRequest.PullRequest:
    # Find pull requests for the branch (assuming you're looking for open PRs)
    pulls = repo.get_pulls(state="open", sort="created", head=f"{repo.owner.login}:{branch_name}")
    pulls = list(pulls)

    if len(pulls) == 0:
        raise AssertionError(f"No open PR found for branch {branch_name}")
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


def github_app_access_token():
    assert config.OWIDBOT_APP_CLIENT_ID, "OWIDBOT_APP_CLIENT_ID is not set"
    assert config.OWIDBOT_APP_PRIVATE_KEY_PATH, "OWIDBOT_APP_PRIVATE_KEY_PATH is not set"
    assert config.OWIDBOT_APP_INSTALLATION_ID, "OWIDBOT_APP_INSTALLATION_ID is not set"

    jwt_token = generate_jwt(config.OWIDBOT_APP_CLIENT_ID, config.OWIDBOT_APP_PRIVATE_KEY_PATH)

    # Use the JWT to get an installation access token
    headers = {"Authorization": f"Bearer {jwt_token}", "Accept": "application/vnd.github.v3+json"}

    installation_access_token_url = (
        f"https://api.github.com/app/installations/{config.OWIDBOT_APP_INSTALLATION_ID}/access_tokens"
    )
    response = requests.post(installation_access_token_url, headers=headers)
    response.raise_for_status()
    access_token = response.json()["token"]

    return access_token

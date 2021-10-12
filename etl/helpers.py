#
#  helpers.py
#  etl
#

from contextlib import contextmanager
import tempfile
from typing import Any, Iterator, List, cast

import requests


@contextmanager
def downloaded(url: str) -> Iterator[str]:
    """
    Download the url to a temporary file and yield the filename.
    """
    with tempfile.NamedTemporaryFile() as tmp:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            chunk_size = 2 ** 16  # 64k
            for chunk in r.iter_content(chunk_size=chunk_size):
                tmp.write(chunk)

        yield tmp.name


def get_latest_github_sha(org: str, repo: str, branch: str) -> str:
    # Use Github's list-branches API to get the sha1 of the most recent commit
    # https://docs.github.com/en/rest/reference/repos#list-branches
    branches = _get_github_branches(org, repo)
    (match,) = [b for b in branches if b["name"] == branch]
    return cast(str, match["commit"]["sha"])


def _get_github_branches(org: str, repo: str) -> Iterator[Any]:
    page = 1

    branches = _get_github_branches_page(org, repo, page)
    yield from branches

    while len(branches) == 100:
        page += 1

        branches = _get_github_branches_page(org, repo, page)
        yield from branches


def _get_github_branches_page(org: str, repo: str, page: int = 1) -> List[Any]:
    url = f"https://api.github.com/repos/{org}/{repo}/branches?per_page=100&page={page}"
    resp = requests.get(url, headers={"Accept": "application/vnd.github.v3+json"})
    if resp.status_code != 200:
        raise Exception(f"got {resp.status_code} from {url}")

    return cast(List[Any], resp.json())

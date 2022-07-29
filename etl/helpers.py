#
#  helpers.py
#  etl
#

from concurrent.futures import ProcessPoolExecutor
import tempfile
from contextlib import contextmanager
from typing import Any, Iterator, List, cast, Callable

import requests


@contextmanager
def downloaded(url: str) -> Iterator[str]:
    """
    Download the url to a temporary file and yield the filename.
    """
    with tempfile.NamedTemporaryFile() as tmp:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            chunk_size = 2**16  # 64k
            for chunk in r.iter_content(chunk_size=chunk_size):
                tmp.write(chunk)

        yield tmp.name


def get_etag(url: str) -> str:
    resp = requests.head(url)
    resp.raise_for_status()
    return cast(str, resp.headers["ETag"])


def get_latest_github_sha(org: str, repo: str, branch: str) -> str:
    # Use Github's list-branches API to get the sha1 of the most recent commit
    # https://docs.github.com/en/rest/reference/repos#list-branches
    branches = _get_github_branches(org, repo)
    (match,) = [b for b in branches if b["name"] == branch]
    return cast(str, match["commit"]["sha"])


def _get_github_branches(org: str, repo: str) -> List[Any]:
    url = f"https://api.github.com/repos/{org}/{repo}/branches?per_page=100"
    resp = requests.get(url, headers={"Accept": "application/vnd.github.v3+json"})
    if resp.status_code != 200:
        raise Exception(f"got {resp.status_code} from {url}")

    branches = cast(List[Any], resp.json())
    if len(branches) == 100:
        raise Exception("reached single page limit, should paginate request")

    return branches


def run_isolated(f: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """
    Run a function in a separate process.
    """
    with ProcessPoolExecutor(1) as executor:
        return executor.submit(f, *args, **kwargs).result()

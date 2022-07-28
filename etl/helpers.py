#
#  helpers.py
#  etl
#

import re
import sys
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
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


@contextmanager
def isolated_env(
    working_dir: Path, keep_modules: str = r"openpyxl|pyarrow|lxml|PIL"
) -> Generator[None, None, None]:
    """Add given directory to pythonpath, run code in context, and
    then remove from pythonpath and unimport modules imported in context.

    Note that unimporting modules means they'll have to be imported again, but
    it has minimal impact on performance (ms).

    :param keep_modules: regex of modules to keep imported
    """
    # add module dir to pythonpath
    sys.path.append(working_dir.as_posix())

    # remember modules that were imported before
    imported_modules = set(sys.modules.keys())

    yield

    # unimport modules imported during execution unless they match `keep_modules`
    for module_name in set(sys.modules.keys()) - imported_modules:
        if not re.search(keep_modules, module_name):
            sys.modules.pop(module_name)

    # remove module dir from pythonpath
    sys.path.remove(working_dir.as_posix())

#
#  helpers.py
#  etl
#

import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, List, cast

import requests
from owid import catalog, walden

from etl import paths


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


class Names:
    """Helper object with naming conventions. It uses your module path (__file__) and
    extracts from it commonly used attributes like channel / namespace / version / short_name or
    paths to datasets from different channels.

    Usage:
        N = Names(__file__)
        ds_garden = N.garden_dataset
    """

    def __init__(self, __file__: str):
        self.f = Path(__file__)

    @property
    def channel(self) -> str:
        return self.f.parent.parent.parent.name

    @property
    def namespace(self) -> str:
        return self.f.parent.parent.name

    @property
    def version(self) -> str:
        return self.f.parent.name

    @property
    def short_name(self) -> str:
        return self.f.stem

    @property
    def country_mapping_path(self) -> Path:
        return self.f.parent / (self.short_name + ".countries.json")

    @property
    def excluded_countries_path(self) -> Path:
        return self.f.parent / (self.short_name + ".excluded_countries.json")

    @property
    def metadata_path(self) -> Path:
        return self.f.parent / (self.short_name + ".meta.yml")

    @property
    def meadow_dataset(self) -> catalog.Dataset:
        return catalog.Dataset(
            paths.DATA_DIR / f"meadow/{self.namespace}/{self.version}/{self.short_name}"
        )

    @property
    def garden_dataset(self) -> catalog.Dataset:
        return catalog.Dataset(
            paths.DATA_DIR / f"garden/{self.namespace}/{self.version}/{self.short_name}"
        )

    @property
    def walden_dataset(self) -> walden.Dataset:
        return walden.Catalog().find_one(
            namespace=self.namespace, version=self.version, short_name=self.short_name
        )

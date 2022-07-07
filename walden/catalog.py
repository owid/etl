"""Prototype."""


from os import path, makedirs, unlink as delete
from dataclasses import dataclass
import datetime as dt
from typing import Any, Dict, Optional, Iterator, List, Tuple, Union, Literal
import json
import yaml
import shutil
from pathlib import Path

from dataclasses_json import dataclass_json

from . import owid_cache, files

# this repository
BASE_DIR = path.dirname(__file__)

# our folder of JSON documents
# TODO: better path
INDEX_DIR = path.abspath(path.join(BASE_DIR, "../etl/steps/data/walden"))

# our local copy
# TODO: better path
CACHE_DIR = path.abspath(path.join(BASE_DIR, "../data/walden"))


# the JSONschema that they must match
SCHEMA_FILE = path.join(BASE_DIR, "schema.json")


@dataclass_json
@dataclass
class WaldenDataset:
    """
    A specific dataset represented by a data file plus metadata.
    If there are multiple versions, this is just one of them.

    Construct it from a dictionary or JSON:

        > WaldenDataset.from_dict({"md5": "2342332", ...})
        > WaldenDataset.from_json('{"md5": "23423432", ...}')

    Then you can fetch the file of the dataset with:

        > filename = WaldenDataset.ensure_downloaded()

    and begin working with that file.
    """

    # how we identify the dataset
    namespace: str  # a short source name (usually institution name)
    short_name: str  # a slug, ideally unique, snake_case, no spaces

    # fields that are meant to be shown to humans
    name: str
    description: str
    source_name: str
    url: str

    # how to get the data file
    file_extension: str

    # today by default
    date_accessed: str = dt.datetime.now().date().strftime("%Y-%m-%d")

    # URL with file, use `download_and_create(metadata)` for uploading to walden
    source_data_url: Optional[str] = None

    # license
    # NOTE: license_url should be ideally required, but we don't have it for backported datasets
    # so we have to relax this condition
    license_url: Optional[str] = None
    license_name: Optional[str] = None
    access_notes: Optional[str] = None

    is_public: Optional[bool] = True

    # use either publication_year or publication_date as dataset version if not given explicitly
    version: Optional[str] = None
    publication_year: Optional[int] = None
    publication_date: Union[Optional[dt.date], Literal["latest"]] = None

    # md5 of the origin, can differ from `md5` attribute, used for internal purposes only
    origin_md5: Optional[str] = None

    # fields that are not meant to be set in metadata and are computed on the fly
    owid_data_url: Optional[str] = None
    md5: Optional[str] = None

    def __post_init__(self) -> None:
        if self.version is None:
            if self.publication_date:
                self.version = str(self.publication_date)
            elif self.publication_year:
                self.version = str(self.publication_year)
            else:
                raise ValueError("no versioning field found")

    @classmethod
    def download_and_create(
        cls, metadata: Union[dict, "WaldenDataset"]
    ) -> "WaldenDataset":
        if isinstance(metadata, dict):
            dataset = Dataset.from_dict(metadata)  # type: ignore
        else:
            dataset = metadata

        # make sure we have a local copy
        filename = dataset.ensure_downloaded()

        # set the md5
        dataset.md5 = files.checksum(filename)

        return dataset

    @classmethod
    def copy_and_create(
        cls, filename: str, metadata: Union[dict, "WaldenDataset"]
    ) -> "WaldenDataset":
        """
        Create a new dataset if you already have the file locally.
        """
        if isinstance(metadata, dict):
            dataset = Dataset.from_dict(metadata)  # type: ignore
        else:
            dataset = metadata

        # set the md5
        dataset.md5 = files.checksum(filename)

        # copy the file into the cache
        dataset.add_to_cache(filename)

        return dataset

    @classmethod
    def from_file(cls, filename: str) -> "WaldenDataset":
        with open(filename) as istream:
            return cls.from_json(istream.read())  # type: ignore

    @classmethod
    def from_yaml(cls, filename: Union[str, Path]) -> "WaldenDataset":
        with open(filename) as istream:
            meta = yaml.safe_load(istream)
            return cls(**meta)

    def add_to_cache(self, filename: str) -> None:
        """
        Copy the pre-downloaded file into the cache. This avoids having to
        redownload it if you already have a copy.
        """
        cache_file = self.local_path

        # make the parent folder
        create(cache_file)
        shutil.copy(filename, cache_file)

    @property
    def metadata(self) -> Dict[str, Any]:
        # prune any keys with empty values
        return {k: v for k, v in self.to_dict().items() if v is not None}

    def save(self) -> None:
        "Save any changes as JSON to the catalog."
        create(self.index_path)
        with open(self.index_path, "w") as ostream:
            print(json.dumps(self.metadata, indent=2, default=str), file=ostream)  # type: ignore

    def delete(self) -> None:
        """
        Remove this dataset record from the local catalog. It will still remain on Github
        unless this change is committed and pushed there. Mostly useful for testing.
        """
        if path.exists(self.index_path):
            delete(self.index_path)

    @property
    def index_path(self) -> str:
        return path.join(INDEX_DIR, f"{self.relative_base}.json")

    @property
    def relative_base(self):
        return path.join(self.namespace, self.version, f"{self.short_name}")

    def ensure_downloaded(self, quiet=False) -> str:
        "Download it if it hasn't already been downloaded. Return the local file path."
        filename = self.local_path
        if not path.exists(filename):
            # make the parent folder
            create(filename)

            # actually get it
            url = self.owid_data_url or self.source_data_url
            if not url:
                raise Exception(
                    f"dataset {self.name} has neither source_data_url nor owid_data_url"
                )
            if self.is_public:
                files.download(url, filename, expected_md5=self.md5, quiet=quiet)
            else:
                owid_cache.download(url, filename, expected_md5=self.md5, quiet=quiet)

        return filename

    def upload(self, public: bool = False) -> None:
        """
        Copy the local file to our cache. It updates the `owid_data_url` field.
        """
        # download the file to the local cache if we don't have it already
        self.ensure_downloaded()

        # add it to our remote cache of data files
        dest_path = f"{self.relative_base}.{self.file_extension}"
        cache_url = owid_cache.upload(self.local_path, dest_path, public=public)

        # remember how to access it
        self.owid_data_url = cache_url

        self.is_public = public

    @property
    def local_path(self) -> str:
        return path.join(CACHE_DIR, f"{self.relative_base}.{self.file_extension}")

    def to_dict(self) -> Dict[str, Any]:
        ...


class Catalog:
    def __init__(self):
        self.datasets: List[WaldenDataset] = []
        self.refresh()

    def refresh(self):
        self.datasets = [WaldenDataset.from_dict(d) for _, d in iter_docs()]  # type: ignore

    def __iter__(self):
        yield from iter(self.datasets)

    def __len__(self):
        return len(self.datasets)

    def find(
        self,
        namespace: Optional[str] = None,
        version: Optional[str] = None,
        short_name: Optional[str] = None,
    ) -> List[WaldenDataset]:
        results = []
        for dataset in self:
            if (
                (not namespace or dataset.namespace == namespace)
                and (not version or dataset.version == version)
                and (not short_name or dataset.short_name == short_name)
            ):
                results.append(dataset)

        return results

    def find_one(
        self,
        namespace: Optional[str] = None,
        version: Optional[str] = None,
        short_name: Optional[str] = None,
    ) -> WaldenDataset:
        matches = self.find(namespace=namespace, version=version, short_name=short_name)

        if len(matches) > 1:
            raise Exception("too many matches for dataset")
        elif len(matches) == 0:
            raise KeyError("no match for dataset")

        return matches[0]

    def find_latest(
        self,
        namespace: Optional[str] = None,
        short_name: Optional[str] = None,
    ) -> WaldenDataset:
        matches = self.find(namespace=namespace, short_name=short_name)
        _, dataset = max((d.version, d) for d in matches)
        return dataset


def load_schema() -> dict:
    with open(SCHEMA_FILE) as istream:
        return json.load(istream)


def iter_docs() -> Iterator[Tuple[str, dict]]:
    return files.iter_docs(INDEX_DIR)


def create(filename) -> None:
    """
    Create directory to file. E.g., for filename 'a/b/c/file.csv' it will make sure 'a/b/c' exists.
    """
    parent_dir = path.dirname(filename)
    if not path.isdir(parent_dir):
        makedirs(parent_dir)

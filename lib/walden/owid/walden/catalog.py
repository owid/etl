"""Prototype."""

import datetime as dt
import json
import shutil
from dataclasses import dataclass
from os import makedirs, path
from os import unlink as delete
from pathlib import Path
from typing import Any, Dict, Iterator, List, Literal, Optional, Tuple, Union

import yaml
from dataclasses_json import dataclass_json
from structlog import get_logger

from . import files, owid_cache

# our local copy
CACHE_DIR = path.expanduser("~/.owid/walden")

# this repository
BASE_DIR = path.dirname(__file__)

# our folder of JSON documents
INDEX_DIR = path.abspath(path.join(BASE_DIR, "index"))

# the JSONschema that they must match
SCHEMA_FILE = path.join(BASE_DIR, "schema.json")

log = get_logger()


@dataclass_json
@dataclass
class Dataset:
    """
    A specific dataset represented by a data file plus metadata.
    If there are multiple versions, this is just one of them.

    Construct it from a dictionary or JSON:

        > Dataset.from_dict({"md5": "2342332", ...})
        > Dataset.from_json('{"md5": "23423432", ...}')

    Then you can fetch the file of the dataset with:

        > filename = Dataset.ensure_downloaded()

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
        else:
            # version can be loaded as datetime.date, but it has to be string
            self.version = str(self.version)

    @classmethod
    def download_and_create(cls, metadata: Union[dict, "Dataset"]) -> "Dataset":
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
    def copy_and_create(cls, filename: str, metadata: Union[dict, "Dataset"]) -> "Dataset":
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
    def from_file(cls, filename: str) -> "Dataset":
        with open(filename) as istream:
            return cls.from_json(istream.read())  # type: ignore

    @classmethod
    def from_yaml(cls, filename: Union[str, Path]) -> "Dataset":
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
        assert self.version
        return path.join(self.namespace, self.version, f"{self.short_name}")

    def ensure_downloaded(self, quiet=False) -> str:
        "Download it if it hasn't already been downloaded and matches checksum. Return the local file path."
        filename = self.local_path

        if self.md5 and path.exists(filename) and files.checksum(filename) == self.md5:
            return filename
        else:
            # make the parent folder
            create(filename)

            # actually get it
            url = self.owid_data_url or self.source_data_url
            if not url:
                raise Exception(f"dataset {self.name} has neither source_data_url nor owid_data_url")
            if self.is_public:
                files.download(url, filename, expected_md5=self.md5, quiet=quiet)
            else:
                owid_cache.download(url, filename, expected_md5=self.md5, quiet=quiet)

        return filename

    def upload(self, public: bool = False, check_changed: bool = False) -> bool:
        """Copy the local file to our cache. It updates the `owid_data_url` field.

        Arguments:
        ----------
        public: bool
            If True, the file will be uploaded to the public database. Otherwise, it will be uploaded to the private database. Defaults to False.
        check_changed: bool
            If True, the file will only be uploaded if it has changed since the last upload. Defaults to False.

        Returns:
        --------
        bool:
            True if the file was uploaded, False otherwise.
        """
        if (check_changed and self.has_changed_from_last_version()) or not check_changed:
            # download the file to the local cache if we don't have it already
            self.ensure_downloaded()

            # add it to our remote cache of data files
            dest_path = f"{self.relative_base}.{self.file_extension}"
            cache_url = owid_cache.upload(self.local_path, dest_path, public=public)

            # remember how to access it
            self.owid_data_url = cache_url

            # Set attribute to public
            self.is_public = public

            # Return True because the file was uploaded
            return True
        # Return False because the file was not uploaded
        return False

    def upload_and_save(self, upload: bool, public: bool = False, check_changed: bool = True) -> None:
        """Update index and upload dataset if required.

        Parameters
        ----------
        upload : bool
            Set to True to upload dataset to Walden.
        public: bool
            If True, the file will be uploaded to the public database. Otherwise, it will be uploaded to the private database. Defaults to False.
        check_changed: bool
            If True, the file will only be uploaded if it has changed since the last upload. Defaults to False.
        """
        # Upload dataset
        if upload:
            is_uploaded = self.upload(public=public, check_changed=check_changed)
            if is_uploaded:
                self.save()
        else:
            # Save index
            if (check_changed and self.has_changed_from_last_version()) or not check_changed:
                self.save()

    def delete_from_remote(self) -> None:
        """
        Delete the file from the remote cache on S3.
        """
        dest_path = f"{self.relative_base}.{self.file_extension}"
        owid_cache.delete(dest_path)

    @property
    def local_path(self) -> str:
        return path.join(CACHE_DIR, f"{self.relative_base}.{self.file_extension}")

    def to_dict(self) -> Dict[str, Any]: ...  # type: ignore

    def has_changed_from_last_version(self) -> bool:
        """Check if local dataset is different to latest available version in Walden.

        Retrieves last version of the dataset in Walden and compares it to the current version. Comparison is done by
        string comparing the MD5 checksums of the two datasets.

        Parameters
        ----------
        dataset : Dataset
            Dataset that was just retrieved.

        Returns
        -------
        bool
            True if dataset in Walden is different to the self.
        """
        if self.md5:
            try:
                dataset_last = Catalog().find_latest(namespace=self.namespace, short_name=self.short_name)
            except ValueError:
                is_different = True
            else:
                is_different = dataset_last.md5 != self.md5
        else:
            raise ValueError(
                "no md5 to check! Make sure you have correctly created the dataset. See methods `download_and_create`,"
                " `copy_and_create`"
            )

        # Logging
        if is_different:
            log.info("Updating dataset!")
        else:
            log.info("Update not needed.")

        return is_different


class Catalog:
    def __init__(self):
        self.datasets: List[Dataset] = []
        self.refresh()

    def refresh(self):
        self.datasets = [Dataset.from_dict(d) for _, d in iter_docs()]  # type: ignore

    def __iter__(self):
        yield from iter(self.datasets)

    def __len__(self):
        return len(self.datasets)

    def find(
        self,
        namespace: Optional[str] = None,
        version: Optional[str] = None,
        short_name: Optional[str] = None,
    ) -> List[Dataset]:
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
    ) -> Dataset:
        matches = self.find(namespace=namespace, version=version, short_name=short_name)

        if len(matches) > 1:
            raise Exception("too many matches for dataset")
        elif len(matches) == 0:
            raise KeyError(f"no match for dataset {namespace}/{version}/{short_name}")

        return matches[0]

    def find_latest(
        self,
        namespace: str,
        short_name: str,
    ) -> Dataset:
        matches = self.find(namespace=namespace, short_name=short_name)
        if not matches:
            raise ValueError(f"Dataset {short_name} in namespace {namespace} not found in walden")
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
    makedirs(parent_dir, exist_ok=True)

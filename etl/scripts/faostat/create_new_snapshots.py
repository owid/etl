"""Ingest FAOSTAT data and create a snapshot of each FAOSTAT domain dataset.

This script will check if the snapshots of FAOSTAT datasets we have are up-to-date. If any of the individual domain
datasets is not up-to-date, this script will create a new snapshot folder with snapshots of the latest version.

Snapshots will be stored as:
* .zip files, for data of each of the individual domains (e.g. faostat_qcl.zip).
* .json files, for metadata (faostat_metadata.json).

Usage:
* To show available options:
```
poetry run python -m create_new_snapshots -h
```
* To simply check if any of the datasets needs to be updated (without actually creating snapshots):
```
poetry run python -m create_new_snapshots -r
```
* To check for updates and actually create new snapshots:
```
poetry run python -m create_new_snapshots
```

"""

import argparse
import datetime as dt
import json
import tempfile
from pathlib import Path
from typing import Any, Dict, List, cast

import requests
import ruamel.yaml
from dateutil import parser

from etl.paths import SNAPSHOTS_DIR
from etl.scripts.faostat.shared import (
    API_BASE_URL,
    FAO_CATALOG_URL,
    FAO_DATA_URL,
    INCLUDED_DATASETS_CODES,
    LICENSE_NAME,
    LICENSE_URL,
    NAMESPACE,
    SOURCE_NAME,
    VERSION,
    log,
)
from etl.snapshot import Snapshot, SnapshotMeta, add_snapshot, snapshot_catalog


def create_snapshot_metadata_file(metadata: Dict[str, Any]) -> None:
    # Path to new snapshot folder.
    snapshot_dir_path = Path(SNAPSHOTS_DIR / metadata["namespace"] / metadata["version"])
    # Path to new snapshot metadata file.
    snapshot_file_path = (snapshot_dir_path / metadata["short_name"]).with_suffix(f".{metadata['file_extension']}.dvc")

    # Ensure new snapshot folder exists, otherwise create it.
    snapshot_dir_path.mkdir(exist_ok=True)

    # Create metadata file for current domain dataset.
    with open(snapshot_file_path, "w") as f:
        ruamel.yaml.dump({"meta": metadata}, f, Dumper=ruamel.yaml.RoundTripDumper)


class FAODataset:
    namespace: str = NAMESPACE

    def __init__(self, dataset_metadata: Dict[str, Any]) -> None:
        """[summary]

        Args:
            dataset_metadata (dict): Dataset raw metadata.
        """
        self._dataset_metadata = dataset_metadata
        self._dataset_server_metadata = self._load_dataset_server_metadata()

    def _load_dataset_server_metadata(self) -> Dict[str, Any]:
        # Fetch only header of the dataset file on the server, which contains additional metadata, like last
        # modification date.
        head_request = requests.head(self.source_data_url)
        dataset_header = head_request.headers
        return cast(dict, dataset_header)

    @property
    def publication_year(self) -> int:
        return self.publication_date.year

    @property
    def publication_date(self) -> dt.date:
        return dt.datetime.fromisoformat(self._dataset_metadata["DateUpdate"]).date()

    @property
    def modification_date(self) -> dt.date:
        last_update_date_str = self._dataset_server_metadata["Last-modified"]
        last_update_date = parser.parse(last_update_date_str).date()
        return last_update_date

    @property
    def short_name(self) -> str:
        return f"{self.namespace}_{self._dataset_metadata['DatasetCode'].lower()}"

    @property
    def source_data_url(self) -> str:
        return self._dataset_metadata["FileLocation"]

    @property
    def metadata(self) -> Dict[str, Any]:
        """
        Snapshot-compatible view of this dataset's metadata.

        """
        if self._dataset_metadata["DatasetDescription"] is None:
            # Description is sometimes missing (e.g. in faostat_esb), but a description is required in index.
            self._dataset_metadata["DatasetDescription"] = ""
            log.warning(f"Description for dataset {self.short_name} is missing. Type one manually.")
        return {
            "namespace": self.namespace,
            "short_name": self.short_name,
            "name": (f"{self._dataset_metadata['DatasetName']} - FAO" f" ({self.publication_year})"),
            "description": self._dataset_metadata["DatasetDescription"],
            "source_name": SOURCE_NAME,
            "source_published_by": SOURCE_NAME,
            "publication_year": self.publication_year,
            "publication_date": str(self.publication_date),
            "date_accessed": VERSION,
            "version": VERSION,
            "url": f"{FAO_DATA_URL}/{self._dataset_metadata['DatasetCode']}",
            "source_data_url": self.source_data_url,
            "file_extension": "zip",
            "license_url": LICENSE_URL,
            "license_name": LICENSE_NAME,
            "is_public": True,
        }

    @property
    def snapshot_metadata(self) -> SnapshotMeta:
        return SnapshotMeta(**self.metadata)

    def to_snapshot(self) -> None:
        """
        Create a snapshot.

        Download the dataset from the source, create the corresponding metadata file and create the snapshot.
        """
        log.info(f"Creating snapshot for step {self.metadata['short_name']}.")

        # Create metadata file for current domain dataset.
        create_snapshot_metadata_file(self.metadata)

        # Create a new snapshot.
        snap = Snapshot(self.snapshot_metadata.uri)

        # Download data from source.
        snap.download_from_source()

        # Add file to DVC and upload to S3.
        snap.dvc_add(upload=True)


def load_faostat_catalog() -> List[Dict[str, Any]]:
    # Some of the texts returned have special characters that seem to require CP-1252 decoding.
    # datasets = requests.get(FAO_CATALOG_URL).json()["Datasets"]["Dataset"]
    datasets = json.loads(requests.get(FAO_CATALOG_URL).content.decode("cp1252"))["Datasets"]["Dataset"]
    return datasets


def is_dataset_already_up_to_date(
    existing_snapshots: List[Snapshot], source_data_url: str, source_modification_date: dt.date
) -> bool:
    """Check if our latest snapshot for a particular domain dataset is already up-to-date.

    Iterate over all snapshots in the catalog and check if:
    * The URL of the source data coincides with the one of the current domain dataset.
    * The last time the source data was accessed was more recently than the source's last modification date.

    If those conditions are fulfilled, we consider that the current dataset does not need to be updated.

    Args:
        source_data_url (str): URL of the source data.
        source_modification_date (date): Last modification date of the source dataset.
    """
    dataset_up_to_date = False
    for snapshot in existing_snapshots:
        snapshot_source_data_url = snapshot.metadata.source_data_url
        snapshot_date_accessed = parser.parse(str(snapshot.metadata.date_accessed)).date()
        if (snapshot_source_data_url == source_data_url) and (snapshot_date_accessed > source_modification_date):
            dataset_up_to_date = True

    return dataset_up_to_date


class FAOAdditionalMetadata:
    def __init__(self) -> None:
        # Assign current date to additional metadata.
        self.publication_date = dt.datetime.today().date()
        self.publication_year = self.publication_date.year

        # Initialise the combined metadata to be downloaded from FAOSTAT.
        self.faostat_metadata = None

    @property
    def metadata(self) -> Dict[str, Any]:
        return {
            "namespace": NAMESPACE,
            "short_name": f"{NAMESPACE}_metadata",
            "name": f"Metadata and identifiers - FAO ({self.publication_year})",
            "description": "Metadata and identifiers used in FAO datasets",
            "source_name": SOURCE_NAME,
            "source_published_by": SOURCE_NAME,
            "publication_year": self.publication_year,
            "publication_date": str(self.publication_date),
            "date_accessed": VERSION,
            "version": VERSION,
            "url": FAO_DATA_URL,
            "source_data_url": None,
            "file_extension": "json",
            "license_url": LICENSE_URL,
            "license_name": LICENSE_NAME,
            "is_public": True,
        }

    @property
    def snapshot_metadata(self) -> SnapshotMeta:
        return SnapshotMeta(**self.metadata)

    @staticmethod
    def _fetch_additional_metadata_and_save(output_filename: str) -> None:
        faostat_metadata = {}
        # Fetch additional metadata for each domain and category using API.
        for domain in INCLUDED_DATASETS_CODES:
            log.info(f"Fetching additional metadata for domain {domain}.")
            domain_meta = {}
            # Get list of categories (e.g. "items", "element", etc.) for this dataset.
            response = requests.get(f"{API_BASE_URL}/{domain}")
            assert response.ok, f"Failed to fetch API data for dataset {domain}."
            categories = [field["code"] for field in json.loads(response.content)["data"]]
            for category in categories:
                resp = requests.get(f"{API_BASE_URL}/{domain}/{category}")
                if resp.ok:
                    domain_meta[category] = resp.json()

            faostat_metadata[domain] = domain_meta

        # Save additional metadata to temporary local file.
        with open(output_filename, "w") as _output_filename:
            json.dump(faostat_metadata, _output_filename, indent=2, sort_keys=True)

    def to_snapshot(self) -> None:
        log.info(f"Creating snapshot for step {self.metadata['short_name']}.")

        # Create metadata file for current domain dataset.
        create_snapshot_metadata_file(self.metadata)

        with tempfile.NamedTemporaryFile() as f:
            # Download data into a temporary file.
            self._fetch_additional_metadata_and_save(f.name)

            # Create snapshot.
            add_snapshot(uri=self.snapshot_metadata.uri, filename=f.name, upload=True)


def main(read_only: bool = False) -> None:
    # Load list of existing snapshots related to current NAMESPACE.
    existing_snapshots = snapshot_catalog(match=NAMESPACE)

    # Initialise a flag that will become true if any dataset needs to be updated.
    any_dataset_was_updated = False
    # Fetch dataset codes from FAOSTAT catalog.
    faostat_catalog = load_faostat_catalog()
    for description in faostat_catalog:
        # Build FAODataset instance.
        dataset_code = description["DatasetCode"].lower()
        if dataset_code in INCLUDED_DATASETS_CODES:
            faostat_dataset = FAODataset(description)
            if is_dataset_already_up_to_date(
                existing_snapshots=existing_snapshots,
                source_data_url=faostat_dataset.source_data_url,
                source_modification_date=faostat_dataset.modification_date,
            ):
                # Skip dataset if it already is up-to-date in index.
                log.info(f"Dataset {dataset_code} is already up-to-date.")
            else:
                log.warning(f"Dataset {dataset_code} needs to be updated.")
                if not read_only:
                    # Download dataset, upload file to walden bucket and add metadata file to walden index.
                    faostat_dataset.to_snapshot()
                any_dataset_was_updated = True

    # Fetch additional metadata only if at least one dataset was updated.
    if any_dataset_was_updated:
        log.warning("Additional metadata needs to be fetched.")
        if not read_only:
            additional_metadata = FAOAdditionalMetadata()
            # Fetch additional metadata from FAO API, upload file to S3 and add metadata file to walden index.
            additional_metadata.to_snapshot()
    else:
        log.info("No need to fetch additional metadata, since all datasets are up-to-date.")


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description=__doc__)
    argument_parser.add_argument(
        "-r",
        "--read_only",
        default=False,
        action="store_true",
        help="If given, simply check for updates without creating snapshots.",
    )
    args = argument_parser.parse_args()
    main(read_only=args.read_only)

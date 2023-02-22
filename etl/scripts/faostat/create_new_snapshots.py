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
from typing import cast

import requests
import ruamel.yaml
from dateutil import parser
from structlog import get_logger

from etl.paths import SNAPSHOTS_DIR
from etl.snapshot import Snapshot, add_snapshot
from owid.walden.catalog import INDEX_DIR
from owid.walden.files import iter_docs

# Initialize logger.
log = get_logger()

# Version tag to assign to new walden folders (both in S3 bucket and in index).
VERSION = str(dt.date.today())
# Global namespace for datasets.
NAMESPACE = "faostat"
# URL where FAOSTAT can be manually accessed (used in metadata, but not to actually retrieve the data).
FAO_DATA_URL = "http://www.fao.org/faostat/en/#data"
# Metadata source name.
SOURCE_NAME = "Food and Agriculture Organization of the United Nations"
# Metadata related to license.
LICENSE_URL = "http://www.fao.org/contact-us/terms/db-terms-of-use/en"
LICENSE_NAME = "CC BY-NC-SA 3.0 IGO"
# Codes of FAOSTAT domains to download from FAO and upload to walden bucket.
# This is the list that will determine the datasets (faostat_*) to be created in all further etl data steps.
INCLUDED_DATASETS_CODES = [
    # Land, Inputs and Sustainability: Fertilizers indicators.
    "ef",
    # Climate Change: Emissions intensities.
    "ei",
    # Land, Inputs and Sustainability: Livestock Patterns.
    "ek",
    # Land, Inputs and Sustainability: Land use indicators.
    "el",
    # Land, Inputs and Sustainability: Livestock Manure.
    "emn",
    # Land, Inputs and Sustainability: Pesticides indicators.
    "ep",
    # Land, Inputs and Sustainability: Soil nutrient budget.
    "esb",
    # Discontinued archives and data series: Food Aid Shipments (WFP).
    "fa",
    # Food Balances: Food Balances (2010-).
    "fbs",
    # Food Balances: Food Balances (-2013, old methodology and population).
    "fbsh",
    # Forestry: Forestry Production and Trade.
    "fo",
    # Food Security and Nutrition: Suite of Food Security Indicators.
    "fs",
    # Land, Inputs and Sustainability: Land Cover.
    "lc",
    # Production: Crops and livestock products.
    "qcl",
    # Production: Production Indices.
    "qi",
    # Production: Value of Agricultural Production.
    "qv",
    # Land, Inputs and Sustainability: Fertilizers by Product.
    "rfb",
    # Land, Inputs and Sustainability: Fertilizers by Nutrient.
    "rfn",
    # Land, Inputs and Sustainability: Land Use.
    "rl",
    # Land, Inputs and Sustainability: Pesticides Use.
    "rp",
    # Land, Inputs and Sustainability: Pesticides Trade.
    "rt",
    # Food Balances: Supply Utilization Accounts.
    "scl",
    # SDG Indicators: SDG Indicators.
    "sdgb",
    # Trade: Crops and livestock products.
    "tcl",
    # Trade: Trade Indices.
    "ti",
]
# URL for dataset codes in FAOSTAT catalog.
# This is the URL used to get the remote location of the actual data files to be downloaded, and the date of their
# latest update.
FAO_CATALOG_URL = "http://fenixservices.fao.org/faostat/static/bulkdownloads/datasets_E.json"
# Base URL of API, used to download metadata (about countries, elements, items, etc.).
API_BASE_URL = "https://fenixservices.fao.org/faostat/api/v1/en/definitions/domain"
# URL of walden repos and of this script (just to be included to walden index files as a reference).
GIT_URL_TO_WALDEN = "https://github.com/owid/walden/"
GIT_URL_TO_THIS_FILE = f"{GIT_URL_TO_WALDEN}blob/master/ingests/faostat.py"


def create_snapshot_metadata_file(metadata):
    # URI of snapshot to be created.
    snapshot_uri = f"{metadata['namespace']}/{metadata['version']}/{metadata['short_name']}.{metadata['file_extension']}"
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

    def __init__(self, dataset_metadata: dict):
        """[summary]

        Args:
            dataset_metadata (dict): Dataset raw metadata.
        """
        self._dataset_metadata = dataset_metadata
        self._dataset_server_metadata = self._load_dataset_server_metadata()

    def _load_dataset_server_metadata(self) -> dict:
        # Fetch only header of the dataset file on the server, which contains additional metadata, like last
        # modification date.
        head_request = requests.head(self.source_data_url)
        dataset_header = head_request.headers
        return cast(dict, dataset_header)

    @property
    def publication_year(self):
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
    def short_name(self):
        return f"{self.namespace}_{self._dataset_metadata['DatasetCode'].lower()}"

    @property
    def source_data_url(self):
        return self._dataset_metadata["FileLocation"]

    @property
    def metadata(self):
        f"""
        Snapshot-compatible view of this dataset's metadata.

        """
        if self._dataset_metadata["DatasetDescription"] is None:
            # Description is sometimes missing (e.g. in faostat_esb), but a description is required in index.
            self._dataset_metadata["DatasetDescription"] = ""
            print(f"WARNING: Description for dataset {self.short_name} is missing. Type one manually.")
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
            "url": FAO_DATA_URL,
            "source_data_url": self.source_data_url,
            "file_extension": "zip",
            "license_url": LICENSE_URL,
            "license_name": LICENSE_NAME,
            "is_public": True,
        }

    def to_snapshot(self):
        """
        Create a snapshot.

        Download the dataset from the source, create the corresponding metadata file and create the snapshot.
        """
        # Create metadata file for current domain dataset.
        create_snapshot_metadata_file(self.metadata)

        # URI of snapshot to be created.
        snapshot_uri = f"{self.metadata['namespace']}/{self.metadata['version']}/{self.metadata['short_name']}.{self.metadata['file_extension']}"

        # Create a new snapshot.
        snap = Snapshot(snapshot_uri)

        # Download data from source.
        snap.download_from_source()


def load_faostat_catalog():
    datasets = requests.get(FAO_CATALOG_URL).json()["Datasets"]["Dataset"]
    return datasets


def is_dataset_already_up_to_date(source_data_url, source_modification_date):
    """Check if a dataset is already up-to-date in the walden index.

    Iterate over all files in walden index and check if:
    * The URL of the source data coincides with the one of the current dataset.
    * The last time the source data was accessed was more recently than the source's last modification date.

    If those conditions are fulfilled, we consider that the current dataset does not need to be updated.

    Args:
        source_data_url (str): URL of the source data.
        source_modification_date (date): Last modification date of the source dataset.
    """
    index_dir = Path(INDEX_DIR) / NAMESPACE
    dataset_up_to_date = False
    for filename, index_file in iter_docs(index_dir):
        index_file_source_data_url = index_file.get("source_data_url")
        index_file_date_accessed = dt.datetime.strptime(index_file.get("date_accessed"), "%Y-%m-%d").date()
        if (index_file_source_data_url == source_data_url) and (index_file_date_accessed > source_modification_date):
            dataset_up_to_date = True

    return dataset_up_to_date


class FAOAdditionalMetadata:
    def __init__(self):
        # Assign current date to additional metadata.
        self.publication_date = dt.datetime.today().date()
        self.publication_year = self.publication_date.year
        
        # Initialise the combined metadata to be downloaded from FAOSTAT.
        self.faostat_metadata = None

    @property
    def metadata(self):
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

    @staticmethod
    def _fetch_additional_metadata_and_save(output_filename):
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

    def to_snapshot(self):
        # Create metadata file for current domain dataset.
        create_snapshot_metadata_file(self.metadata)

        # URI of snapshot to be created.
        snapshot_uri = f"{self.metadata['namespace']}/{self.metadata['version']}/{self.metadata['short_name']}.{self.metadata['file_extension']}"

        with tempfile.NamedTemporaryFile() as f:
            # Download data into a temporary file.
            self._fetch_additional_metadata_and_save(f.name)

            # Create snapshot.
            add_snapshot(uri=snapshot_uri, filename=f.name, upload=True)


def main(read_only=False):
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
                source_data_url=faostat_dataset.source_data_url,
                source_modification_date=faostat_dataset.modification_date,
            ):
                # Skip dataset if it already is up-to-date in index.
                log.info(f"Dataset {dataset_code} is already up-to-date.")
            else:
                log.info(f"Dataset {dataset_code} needs to be updated.")
                if not read_only:
                    # Download dataset, upload file to walden bucket and add metadata file to walden index.
                    faostat_dataset.to_snapshot()
                any_dataset_was_updated = True

    # Fetch additional metadata only if at least one dataset was updated.
    if any_dataset_was_updated:
        log.info("Additional metadata needs to be fetched.")
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

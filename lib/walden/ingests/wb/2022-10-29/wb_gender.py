"""Walden step for WB Gender statistics dataset.

WB maintains the dataset in a stable URL, where new releases overwrite past releases.

To understand for which walden datasets this code has been used note the following:
- For each Walden dataset version there is one corresponding Walden code version.
- For each Walden code version there can be >1 Walden dataset versions.

That is, this code may have generated several walden datasets versions.

To find which code generated your dataset version "YYYY-MM-DD", look for the code version closes in time with that version
backwards.
"""

from datetime import datetime
from pathlib import Path

import click
from structlog import get_logger

from owid.walden import Catalog, Dataset

log = get_logger()


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool) -> None:
    metadata = Dataset.from_yaml(Path(__file__).parent / "wb_gender.meta.yml")

    # download dataset from source_data_url and add the local file to Walden's cache in ~/.owid/walden
    dataset = Dataset.download_and_create(metadata)

    if needs_to_be_updated(dataset):
        log.info("Update needed! Updating dataset...")
        # Update version, publication year
        update_metadata_with_version(dataset)

        # upload it to S3
        if upload:
            dataset.upload(public=True)

        # update PUBLIC walden index with metadata
        dataset.save()
    else:
        log.info("No update needed!")


def needs_to_be_updated(dataset: Dataset) -> bool:
    """Check if dataset needs to be updated.

    Retrieves last version of the dataset in Walden and compares it to the current version. Comparison is done by
    string comparing the MD5 checksums of the two datasets.

    TODO: move to class method of Dataset.

    Parameters
    ----------
    dataset : Dataset
        Dataset that was just retrieved.

    Returns
    -------
    bool
        True if dataset in Walden is outdated and a new version is needed.
    """
    log.info("Checking if dataset needs to be updated...")
    try:
        dataset_last = Catalog().find_latest(namespace=dataset.namespace, short_name=dataset.short_name)
        return dataset_last.md5 != dataset.md5
    except ValueError:
        return True


def update_metadata_with_version(dataset: Dataset) -> Dataset:
    """Update metadata of the dataset with new version.

    It uses dataset.date_accessed to update the following fields:

    - dataset.version: Sets it to dataset.date_accessed.
    - dataset.publication_year: Sets it to year(dataset.date_accessed).
    - dataset.name: Adds '(year)' to the end of the name. E.g. 'Dataset name - Source (2021)'.

    Parameters
    ----------
    dataset : Dataset
        Dataset that was just retrieved.

    Returns
    -------
    Dataset
        Dataset with updated version/date fields.
    """
    year = datetime.strptime(dataset.date_accessed, "%Y-%m-%d").year
    dataset.version = dataset.date_accessed
    dataset.publication_year = year
    dataset.name += f" ({dataset.date_accessed})"
    return dataset


if __name__ == "__main__":
    main()

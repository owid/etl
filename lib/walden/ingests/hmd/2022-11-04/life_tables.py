"""Retrieves life expectancy at 10 from Humanity Mortality Database.

The file from HMD needs to be manually downloaded. This is because it requires registration to their website.
The registration process is simple, and is open for the general public. Once registered, you should be able to
see the list of available files at https://www.mortality.org/Data/ZippedDataFiles.
"""

from datetime import datetime
from pathlib import Path

import click
from structlog import get_logger

from owid.walden import Dataset

log = get_logger()


# Path to metadata file.
METADATA_PATH = Path(__file__).with_suffix(".meta.yml")


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
@click.option(
    "--local-data-file",
    type=str,
    help="Local data file to upload to Walden",
    required=True,
)
def main(upload: bool, local_data_file: str) -> None:
    # Load metadata
    dataset = Dataset.from_yaml(METADATA_PATH)

    # Update version, publication year
    update_metadata_with_version(dataset)

    # Add local data file to dataset
    dataset = Dataset.copy_and_create(str(local_data_file), dataset)

    # Upload it to S3 (optionally) and save index
    dataset.upload_and_save(upload=upload, public=True, check_changed=True)


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

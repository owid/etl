"""Walden step for WB Gender statistics dataset.

WB maintains the dataset in a stable URL, where new releases overwrite past releases.

To understand for which walden datasets this code has been used note the following:
- For each Walden dataset version there is one corresponding Walden code version.
- For each Walden code version there can be >1 Walden dataset versions.

That is, this code may have generated several walden datasets versions.

To find which code generated your dataset version "YYYY-MM-DD", look for the code version closest in time with that version
backwards.
"""

from pathlib import Path

import click
from structlog import get_logger

from owid.walden import Dataset

log = get_logger()


METADATA_FILENAME = Path(__file__).parent / "meta.yml"


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool) -> None:
    metadata = Dataset.from_yaml(METADATA_FILENAME)

    # download dataset from source_data_url and add the local file to Walden's cache in ~/.owid/walden
    dataset = Dataset.download_and_create(metadata)

    # Update version
    dataset.version = dataset.date_accessed

    # Upload dataset
    dataset.upload_and_save(upload=upload, public=True)


if __name__ == "__main__":
    main()

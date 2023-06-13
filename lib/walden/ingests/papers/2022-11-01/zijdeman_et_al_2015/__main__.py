"""Get life expectancy at birth from Zindeman et al. (2015)."""

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
    metadata = Dataset.from_yaml(Path(__file__).parent / "meta.yml")

    # download dataset from source_data_url and add the local file to Walden's cache in ~/.owid/walden
    dataset = Dataset.download_and_create(metadata)

    # Update version
    dataset.version = dataset.date_accessed

    # Upload dataset (dataset will only be updated if it has actually changed since last available version)
    dataset.upload_and_save(upload=upload, public=True)


if __name__ == "__main__":
    main()

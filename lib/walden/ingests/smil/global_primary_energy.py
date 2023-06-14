"""This dataset was generated manually, based on the cited book.
This script will load the local file and uploads it to the S3 walden bucket.

"""

from pathlib import Path

import click

from owid.walden import Dataset


@click.command()
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(path_to_file: str, upload: bool) -> None:
    metadata = Dataset.from_yaml(Path(__file__).parent / "global_primary_energy.meta.yml")

    # Load local data file and add it to Walden's cache in ~/.owid/walden.
    dataset = Dataset.copy_and_create(path_to_file, metadata)

    # Upload file to S3.
    if upload:
        dataset.upload(public=True)

    # update PUBLIC walden index with metadata
    dataset.save()


if __name__ == "__main__":
    main()

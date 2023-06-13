"""Ingest IRENA's Renewable Electricity Capacity and Generation Statistics data, using the Desktop version of their
Download Query Tool.

The ingested data is an xlsm file with different sheets.

"""

from pathlib import Path

import click

from owid.walden import Dataset


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool) -> None:
    metadata = Dataset.from_yaml(Path(__file__).parent / "renewable_electricity_capacity_and_generation.meta.yml")

    # Download dataset from source_data_url.
    dataset = Dataset.download_and_create(metadata)

    # Upload file to S3.
    if upload:
        dataset.upload(public=True)

    # Update Walden index with metadata.
    dataset.save()


if __name__ == "__main__":
    main()

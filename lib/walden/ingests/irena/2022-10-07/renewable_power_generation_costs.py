"""Ingest IRENA's chart data from the Renewable Power Generation Costs."""

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
    # Get metadata about this dataset from the adjacent yaml file.
    metadata = Dataset.from_yaml(Path(__file__).parent / "renewable_power_generation_costs.meta.yml")

    # Download dataset from source_data_url and add the local file to walden's cache in: ~/.owid/walden
    dataset = Dataset.download_and_create(metadata)

    # Upload file to S3.
    if upload:
        dataset.upload(public=True)

    # Create a walden index file.
    dataset.save()


if __name__ == "__main__":
    main()

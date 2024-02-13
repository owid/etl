"""Get data by R. Andrew on CO2 mitigation curves for 1.5 Celsius."""

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
    # Path to metadata file.
    metadata = Dataset.from_yaml(Path(__file__).parent / "co2_mitigation_curves_1p5celsius.meta.yml")

    # Download dataset from source_data_url and add the local file to Walden's cache in: ~/.owid/walden
    dataset = Dataset.download_and_create(metadata)

    # Upload raw data file to S3.
    if upload:
        dataset.upload(public=True)

    # Update PUBLIC walden index with metadata.
    dataset.save()


if __name__ == "__main__":
    main()

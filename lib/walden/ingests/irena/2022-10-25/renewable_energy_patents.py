"""Manually upload a file with IRENA data on renewable energy patents.

* The data was manually downloaded from:
https://irena.sharepoint.com/:x:/s/statistics-public/EaUa5nqXqt1Hmm1XEgmOcWQB5MJxYvS_u7eZi8uwJ3EK1A?e=swd7Au
* This data is also shown in their public Tableau dashboard:
https://public.tableau.com/views/IRENARenewableEnergyPatentsTimeSeries_2_0/ExploreMore

These two are very inconvenient platforms to access data from.

Hopefully, in the near future they will use other, more data-science-friendly ways to host their data.

"""

from pathlib import Path

import click
import pandas as pd

from owid.walden import Dataset
from owid.walden.ingest import add_to_catalog

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
    "--local_data_file",
    type=str,
    help="Local data file to upload to Walden",
    required=True,
)
def main(upload: bool, local_data_file: str) -> None:
    # Load raw data.
    df = pd.read_excel(local_data_file, sheet_name="INSPIRE_data")

    # Get walden metadata.
    dataset = Dataset.from_yaml(METADATA_PATH)

    # Add data to Walden catalog and metadata to Walden index.
    add_to_catalog(metadata=dataset, dataframe=df, upload=upload)

    # Update Walden datasets.
    dataset.save()


if __name__ == "__main__":
    main()

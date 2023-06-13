import tempfile
from pathlib import Path

import click
import pandas as pd

from owid.walden import Dataset

# Link to download the data.
DATA_URL = "https://github.com/g-dolphin/ECP/raw/master/_dataset/coverage/tot_coverage_jurisdiction_CO2.csv"
# Name of output file (which should coincide with this file).
FILE_NAME = Path(__file__).stem


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool) -> None:
    metadata = Dataset.from_yaml(Path(__file__).parent / f"{FILE_NAME}.meta.yml")

    # Read data directly from repository file.
    data = pd.read_csv(DATA_URL)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Save data into a temporary file.
        temp_file = Path(temp_dir) / f"{FILE_NAME}.csv.zip"
        data.to_csv(temp_file, index=False, compression="zip")

        # Add the local temporary file to Walden's cache in ~/.owid/walden, with its metadata.
        dataset = Dataset.copy_and_create(str(temp_file), metadata)

        # Upload file to S3.
        if upload:
            dataset.upload(public=True)

    # Update PUBLIC walden index with metadata.
    dataset.save()


if __name__ == "__main__":
    main()

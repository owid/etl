import tempfile
from pathlib import Path

import click
from shared import WCPD_DATA_DIR, WCPD_URL, extract_data_from_remote_zip_folder
from structlog import get_logger

from owid.walden import Dataset

log = get_logger()
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

    # Download zipped repository, extract data from files, and concatenated them into one dataframe.
    log.info("Download data.")
    data = extract_data_from_remote_zip_folder(zip_url=WCPD_URL, path_to_folder=WCPD_DATA_DIR)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Save data into a temporary file.
        log.info("Save data to a temporary file.")
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

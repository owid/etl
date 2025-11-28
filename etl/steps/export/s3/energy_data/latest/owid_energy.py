"""Export step that uploads the OWID Energy dataset to S3.

The combined datasets include:
* Statistical review of world energy - Energy Institute.
* International energy data - U.S. Energy Information Administration.
* Energy from fossil fuels - The Shift Dataportal.
* Yearly Electricity Data - Ember.
* Primary energy consumption - Our World in Data.
* Fossil fuel production - Our World in Data.
* Energy mix - Our World in Data.
* Electricity mix - Our World in Data.

Additionally, OWID's regions dataset, population dataset and Maddison Project Database on GDP are included.

Outputs:
* The data in three different formats will be uploaded to S3, and will be made publicly available, in:
  * https://owid-public.owid.io/data/energy/owid-energy-data.csv
  * https://owid-public.owid.io/data/energy/owid-energy-data.xlsx
  * https://owid-public.owid.io/data/energy/owid-energy-data.json

"""

import json
import tempfile
from pathlib import Path

import pandas as pd
from owid.catalog import Table, s3_utils
from structlog import get_logger
from tqdm.auto import tqdm

from etl.config import DRY_RUN
from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# S3 bucket name and folder where dataset files will be stored.
S3_BUCKET_NAME = "owid-public"
S3_DATA_DIR = Path("data/energy")

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def save_data_to_json(tb: Table, output_path: str) -> None:
    tb = tb.copy()

    # Initialize output dictionary, that contains one item per country in the data.
    output_dict = {}

    # Each country contains a dictionary, which contains:
    # * "iso_code", which is the ISO code (as a string), if it exists.
    # * "data", which is a list of dictionaries, one per year.
    #   Each dictionary contains "year" as the first item, followed by all other non-nan indicator values for that year.
    for country in sorted(set(tb["country"])):
        # Initialize output dictionary for current country.
        output_dict[country] = {}

        # If there is an ISO code for this country, add it as a new item of the dictionary.
        iso_code = tb[tb["country"] == country].iloc[0]["iso_code"]
        if not pd.isna(iso_code):
            output_dict[country]["iso_code"] = iso_code

        # Create the data dictionary for this country.
        dict_country = tb[tb["country"] == country].drop(columns=["country", "iso_code"]).to_dict(orient="records")
        # Remove all nans.
        data_country = [
            {indicator: value for indicator, value in d_year.items() if not pd.isna(value)} for d_year in dict_country
        ]
        output_dict[country]["data"] = data_country

    # Write dictionary to file as a big json object.
    with open(output_path, "w") as file:
        file.write(json.dumps(output_dict, indent=4))


def run() -> None:
    #
    # Load data.
    #
    # Load the owid_energy dataset from garden, and read its main table.
    ds_energy = paths.load_dataset("owid_energy")
    tb = ds_energy.read("owid_energy")

    #
    # Save outputs.
    #
    # Create a temporary directory for all files to be committed.
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)

        # Create a csv file.
        log.info("Creating csv file.")
        pd.DataFrame(tb).to_csv(temp_dir_path / "owid-energy-data.csv", index=False)

        # Create a json file.
        log.info("Creating json file.")
        save_data_to_json(tb, str(temp_dir_path / "owid-energy-data.json"))

        # Create an excel file.
        log.info("Creating excel file.")
        tb.to_excel(temp_dir_path / "owid-energy-data.xlsx", index=False)

        for file_name in tqdm(["owid-energy-data.csv", "owid-energy-data.xlsx", "owid-energy-data.json"]):
            # Path to local file.
            local_file = temp_dir_path / file_name
            # Path (within bucket) to S3 file.
            s3_file = S3_DATA_DIR / file_name

            if DRY_RUN:
                tqdm.write(f"[DRY RUN] Would upload file {local_file} to S3 bucket {S3_BUCKET_NAME} as {s3_file}.")
            else:
                tqdm.write(f"Uploading file {local_file} to S3 bucket {S3_BUCKET_NAME} as {s3_file}.")
                # Upload file to S3
                s3_utils.upload(f"s3://{S3_BUCKET_NAME}/{str(s3_file)}", local_file, public=True, downloadable=True)

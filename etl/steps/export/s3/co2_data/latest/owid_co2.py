"""Garden step that combines various datasets related to greenhouse emissions and produces the OWID CO2 dataset.

The combined datasets are:
* Global Carbon Budget - Global Carbon Project.
* National contributions to climate change - Jones et al.
* Greenhouse gas emissions by sector - Climate Watch.
* Primary energy consumption - EI & EIA.

Additionally, OWID's regions dataset, population dataset and Maddison Project Database (Bolt and van Zanden, 2023) on
GDP are included.

Outputs:
* The data in three different formats will also be uploaded to S3, and will be made publicly available, in:
  * https://owid-public.owid.io/data/co2/owid-co2-data.csv
  * https://owid-public.owid.io/data/co2/owid-co2-data.xlsx
  * https://owid-public.owid.io/data/co2/owid-co2-data.json

"""

import json
import tempfile
from pathlib import Path

import pandas as pd
from owid.catalog import Table, s3_utils
from structlog import get_logger
from tqdm.auto import tqdm

from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# S3 bucket name and folder where dataset files will be stored.
S3_BUCKET_NAME = "owid-public"
S3_DATA_DIR = Path("data/co2")

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


def prepare_and_save_outputs(tb: Table, temp_dir_path: Path) -> None:
    # Create a csv file.
    log.info("Creating csv file.")
    pd.DataFrame(tb).to_csv(temp_dir_path / "owid-co2-data.csv", index=False)

    # Create a json file.
    log.info("Creating json file.")
    save_data_to_json(tb, str(temp_dir_path / "owid-co2-data.json"))

    # Create an excel file.
    log.info("Creating excel file.")
    with pd.ExcelWriter(temp_dir_path / "owid-co2-data.xlsx", engine="openpyxl") as writer:
        # Write data with automatic metadata codebook.
        tb.to_excel(writer, sheet_name="Data", index=False, with_metadata=True, metadata_sheet_name="Metadata")


def run() -> None:
    #
    # Load data.
    #
    # Load the owid_co2 emissions dataset from garden, and read its main table.
    ds_gcp = paths.load_dataset("owid_co2")
    tb = ds_gcp.read("owid_co2")

    #
    # Save outputs.
    #
    # Create a temporary directory for all files to be committed.
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)

        prepare_and_save_outputs(tb, temp_dir_path=temp_dir_path)

        for file_name in tqdm(["owid-co2-data.csv", "owid-co2-data.xlsx", "owid-co2-data.json"]):
            # Path to local file.
            local_file = temp_dir_path / file_name
            # Path (within bucket) to S3 file.
            s3_file = S3_DATA_DIR / file_name
            tqdm.write(f"Uploading file {local_file} to S3 bucket {S3_BUCKET_NAME} as {s3_file}.")

            # Upload file to S3 and force download instead of displaying in browser
            s3_utils.upload(
                f"s3://{S3_BUCKET_NAME}/{str(s3_file)}",
                local_file,
                public=True,
                downloadable=True,
            )

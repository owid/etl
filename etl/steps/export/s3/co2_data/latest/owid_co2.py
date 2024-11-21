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
  * https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.csv
  * https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.xlsx
  * https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.json

"""
import json
import re
import tempfile
from pathlib import Path

import pandas as pd
from owid.catalog import Origin, Table
from owid.datautils.s3 import S3
from structlog import get_logger
from tqdm.auto import tqdm

from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# Define S3 base URL.
S3_URL = "https://nyc3.digitaloceanspaces.com"
# Profile name to use for S3 client (as defined in .aws/config).
S3_PROFILE_NAME = "default"
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


def remove_details_on_demand(text: str) -> str:
    # Remove references to details on demand from a text.
    # Example: "This is a [description](#dod:something)." -> "This is a description."
    regex = r"\(\#dod\:.*\)"
    if "(#dod:" in text:
        text = re.sub(regex, "", text).replace("[", "").replace("]", "")

    return text


def prepare_codebook(tb: Table) -> pd.DataFrame:
    table = tb.copy()

    # Manually create an origin for the regions dataset.
    regions_origin = [Origin(producer="Our World in Data", title="Regions", date_published=str(table["year"].max()))]

    # Manually edit some of the metadata fields.
    table["country"].metadata.title = "Country"
    table["country"].metadata.description_short = "Geographic location."
    table["country"].metadata.description = None
    table["country"].metadata.unit = ""
    table["country"].metadata.origins = regions_origin
    table["year"].metadata.title = "Year"
    table["year"].metadata.description_short = "Year of observation."
    table["year"].metadata.description = None
    table["year"].metadata.unit = ""
    table["year"].metadata.origins = regions_origin

    ####################################################################################################################
    if table["population"].metadata.description is None:
        print("WARNING: Column population has no longer a description field. Remove this part of the code")
    else:
        table["population"].metadata.description = None

    ####################################################################################################################

    # Gather column names, titles, short descriptions, unit and origins from the indicators' metadata.
    metadata = {"column": [], "description": [], "unit": [], "source": []}
    for column in table.columns:
        metadata["column"].append(column)

        if hasattr(table[column].metadata, "description") and table[column].metadata.description is not None:
            print(f"WARNING: Column {column} still has a 'description' field.")
        # Prepare indicator's description.
        description = ""
        if (
            hasattr(table[column].metadata.presentation, "title_public")
            and table[column].metadata.presentation.title_public is not None
        ):
            description += table[column].metadata.presentation.title_public
        else:
            description += table[column].metadata.title
        if table[column].metadata.description_short:
            description += f" - {table[column].metadata.description_short}"
            description = remove_details_on_demand(description)
        metadata["description"].append(description)

        # Prepare indicator's unit.
        if table[column].metadata.unit is None:
            print(f"WARNING: Column {column} does not have a unit.")
            unit = ""
        else:
            unit = table[column].metadata.unit
        metadata["unit"].append(unit)

        # Gather unique origins of current variable.
        unique_sources = []
        for origin in table[column].metadata.origins:
            # Construct the source name from the origin's attribution.
            # If not defined, build it using the default format "Producer - Data product (year)".
            source_name = (
                origin.attribution
                or f"{origin.producer} - {origin.title or origin.title_snapshot} ({origin.date_published.split('-')[0]})"
            )

            # Add url at the end of the source.
            if origin.url_main:
                source_name += f" [{origin.url_main}]"

            # Add the source to the list of unique sources.
            if source_name not in unique_sources:
                unique_sources.append(source_name)

        # Concatenate all sources.
        sources_combined = "; ".join(unique_sources)
        metadata["source"].append(sources_combined)

    # Create a dataframe with the gathered metadata and sort conveniently by column name.
    codebook = pd.DataFrame(metadata).set_index("column").sort_index()
    # For clarity, ensure column descriptions are in the same order as the columns in the data.
    first_columns = ["country", "year", "iso_code", "population", "gdp"]
    codebook = pd.concat([codebook.loc[first_columns], codebook.drop(first_columns, errors="raise")]).reset_index()

    return codebook


def prepare_and_save_outputs(tb: Table, temp_dir_path: Path) -> None:
    # Create codebook and save it as a csv file.
    log.info("Creating codebook csv file.")
    codebook = prepare_codebook(tb=tb)
    codebook.to_csv(temp_dir_path / "owid-co2-codebook.csv", index=False)

    # Create a csv file.
    log.info("Creating csv file.")
    pd.DataFrame(tb).to_csv(temp_dir_path / "owid-co2-data.csv", index=False, float_format="%.3f")

    # Create a json file.
    log.info("Creating json file.")
    # TODO: Uncomment.
    # save_data_to_json(tb, temp_dir_path / "owid-co2-data.json")

    # Create an excel file.
    log.info("Creating excel file.")
    with pd.ExcelWriter(temp_dir_path / "owid-co2-data.xlsx") as writer:
        tb.to_excel(writer, sheet_name="Data", index=False, float_format="%.3f")
        codebook.to_excel(writer, sheet_name="Metadata")


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load the owid_co2 emissions dataset from garden, and read its main table.
    ds_gcp = paths.load_dataset("owid_co2")
    tb = ds_gcp.read("owid_co2")
    # TODO: Maybe codebook should also be a table of owid_co2 dataset, so we can also load it here.

    #
    # Save outputs.
    #
    # Create a temporary directory for all files to be committed.
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)

        prepare_and_save_outputs(tb, temp_dir_path)

        # Initialise S3 client.
        s3 = S3()
        for file_name in tqdm(["owid-co2-data.csv", "owid-co2-data.xlsx", "owid-co2-data.json"]):
            # Path to local file.
            local_file = temp_dir_path / file_name
            # Path (within bucket) to S3 file.
            s3_file = Path("data/co2") / file_name
            tqdm.write(f"Uploading file {local_file} to S3 bucket {S3_BUCKET_NAME} as {s3_file}.")
            # Upload and make public each of the files.
            s3.upload_to_s3(
                local_path=str(local_file),
                s3_path=f"s3://{S3_BUCKET_NAME}/{str(s3_file)}",
                public=True,
            )

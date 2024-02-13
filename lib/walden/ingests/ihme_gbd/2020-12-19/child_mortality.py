import io
import os
import tempfile
import zipfile
from pathlib import Path
from typing import List

import click
import pandas as pd
import requests
from owid.repack import repack_frame
from structlog import get_logger

from owid.walden import add_to_catalog
from owid.walden.catalog import Dataset

log = get_logger()

BASE_URL = "https://ghdx.healthdata.org/sites/default/files/record-attached-files/"


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool) -> None:
    locations = get_location_hierachies()

    start_years = [1950, 1960, 1970, 1980, 1990, 2000, 2010]
    df_all = []
    metadata = Dataset.from_yaml(Path(__file__).parent / "ihme_child_mortality.meta.yml")

    with tempfile.TemporaryDirectory() as temp_dir:
        for start_year in start_years:
            log.info("Downloading data...", decade=start_year)
            end_year = start_year + 9
            df_num = get_number_and_rates(start_year, end_year, temp_dir, locations)
            df_prob = get_probability_death(start_year, end_year, temp_dir, locations)
            df_all.append(df_num)
            df_all.append(df_prob)
        dataset = pd.concat(df_all)
        # consolidate data
        dataset = repack_frame(dataset)
        data_file = os.path.join(temp_dir, f"data.{metadata.file_extension}")
        dataset = dataset.reset_index()
        dataset.to_feather(data_file)
        add_to_catalog(metadata, data_file, upload=upload)


def get_location_hierachies():
    """
    Download the location hierachies spreadsheet so that we can filter out the small sub-national regions and the higher level regions we don't want.
    """
    location_hierachies = f"{BASE_URL}IHME_GBD_2019_GBD_LOCATION_HIERARCHY_Y2022M06D29.XLSX"
    r = requests.get(location_hierachies).content
    xl = pd.ExcelFile(r)
    lh = xl.parse("Sheet1")
    # We only want the global, some of the high level regions and the country level data, levels 0,1 and 3
    lh = lh[lh["Level"].isin([0, 1, 3])]
    locations = lh["Location ID"].to_list()
    return locations


def get_number_and_rates(start_year: int, end_year: int, temp_dir: str, locations: List[str]) -> pd.DataFrame:
    """
    Download the data on the number and rate of child mortality.
    """
    url = f"{BASE_URL}IHME_GBD_2019_U5M_{start_year}_{end_year}_CT_RT_0.zip"
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(temp_dir)
    csv_file = f"IHME_GBD_2019_U5M_{start_year}_{end_year}_CT_RT_Y2021M09D01.CSV"
    df = pd.read_csv(os.path.join(temp_dir, csv_file))
    df_num = df[df["location_id"].isin(locations)]
    return df_num


def get_probability_death(start_year: int, end_year: int, temp_dir: str, locations: List[str]) -> pd.DataFrame:
    """
    Download the data on the probability of death for different age groups of children.
    """
    url = f"{BASE_URL}IHME_GBD_2019_U5M_{start_year}_{end_year}_POD_0.zip"
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(temp_dir)
    csv_file = f"IHME_GBD_2019_U5M_{start_year}_{end_year}_POD_Y2021M09D01.CSV"
    df = pd.read_csv(os.path.join(temp_dir, csv_file))
    df_prob = df[df["location_id"].isin(locations)]
    return df_prob


if __name__ == "__main__":
    main()

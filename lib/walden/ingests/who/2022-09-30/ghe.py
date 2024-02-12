import json
import os
import tempfile
from pathlib import Path
from typing import List

import click
import pandas as pd
import requests
from owid.repack import repack_frame
from structlog import get_logger

from owid.walden import Dataset, add_to_catalog

log = get_logger()


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool):
    with tempfile.TemporaryDirectory() as temp_dir:
        log.info("Creating metadata...")
        metadata = Dataset.from_yaml(Path(__file__).parent / "who_ghe.meta.yml")
        # Get the list of causes of disease/injury. The data is too big to request all at once.
        causes = get_causes_list()
        # Download the data
        dataset = download_cause_data(causes)
        data_file = os.path.join(temp_dir, f"data.{metadata.file_extension}")
        # Save it locally as a temp feather file.
        dataset.to_feather(data_file)
        add_to_catalog(metadata, data_file, upload=upload)


def get_causes_list() -> List[str]:
    url = "https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_FULL?$apply=groupby((DIM_GHECAUSE_TITLE))"
    res = requests.get(url)
    assert res.ok
    value_json = json.loads(res.content)["value"]
    causes = pd.DataFrame.from_records(value_json)["DIM_GHECAUSE_TITLE"].tolist()
    return causes


def get_cause_data(url) -> pd.DataFrame:
    data_json = requests.get(url).json()
    data_df = pd.DataFrame.from_records(data_json["value"])
    return data_df


def download_cause_data(causes) -> pd.DataFrame:
    all_data = []
    # Request each individual cause and append it. Selecting the following variables: Cause, year, country, age group, sex, DALY count, DALY rate per 100k, deaths, deaths per 100k.
    for cause in causes:
        for year in range(2000, 2020):
            log.info("Downloading...", cause=cause, year=year)
            # Use this url to download data for just the All Ages category and for the both sexes category
            # url = f"https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_FULL?$filter=DIM_GHECAUSE_TITLE%20eq%20%27{cause}%27%20and%20DIM_SEX_CODE%20eq%20%27BTSX%27and%20DIM_AGEGROUP_CODE%20eq%20%27ALLAges%27&$select=DIM_GHECAUSE_TITLE,DIM_YEAR_CODE,DIM_COUNTRY_CODE,DIM_AGEGROUP_CODE,DIM_SEX_CODE,VAL_DALY_COUNT_NUMERIC,VAL_DALY_RATE100K_NUMERIC,VAL_DEATHS_COUNT_NUMERIC,VAL_DEATHS_RATE100K_NUMERIC,FLAG_LEVEL"
            # Use this url to download data for all age groups and sexes
            url = f"https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_FULL?$filter=DIM_GHECAUSE_TITLE%20eq%20%27{cause}%27%20and%20DIM_YEAR_CODE%20eq%20%27{year}%27&$select=DIM_GHECAUSE_TITLE,DIM_YEAR_CODE,DIM_COUNTRY_CODE,DIM_AGEGROUP_CODE,DIM_SEX_CODE,VAL_DALY_COUNT_NUMERIC,VAL_DALY_RATE100K_NUMERIC,VAL_DEATHS_COUNT_NUMERIC,VAL_DEATHS_RATE100K_NUMERIC,FLAG_LEVEL"
            df = get_cause_data(url)
            df = repack_frame(df)
            all_data.append(df)
    # combine dataframes - repack them to make them smaller e.g. use categories where possible. Reset index necessary to save as feather.
    all_df = pd.concat(all_data)
    all_df = all_df.reset_index()

    return all_df


if __name__ == "__main__":
    main()

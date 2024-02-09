import datetime as dt
import json
import os
import tempfile
from collections import defaultdict
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import requests
import yaml
from owid.walden import add_to_catalog
from owid.walden.catalog import Dataset
from structlog import get_logger

BASE_URL = "https://unstats.un.org/sdgapi"
log = get_logger()


URL_METADATA = "https://unstats.un.org/sdgs/indicators/SDG_Updateinfo.xlsx"
MAX_RETRIES = 10
CHUNK_SIZE = 1024 * 1024 * 10


def main():
    log.info("Creating metadata...")
    metadata = create_metadata()
    with tempfile.TemporaryDirectory() as temp_dir:
        # fetch the file locally
        assert metadata.source_data_url is not None
        log.info("Downloading data...")
        all_data = download_data()
        log.info("Saving data...")
        data_file = os.path.join(temp_dir, f"data.{metadata.file_extension}")
        all_data.to_feather(data_file)
        log.info("Adding data to catalog...")
        add_to_catalog(metadata, data_file, upload=True)  # type: ignore

        log.info("Downloading unit descriptions...")
        unit_desc = attributes_description()
        metadata_unit = metadata
        metadata_unit.description = "A description of the units the data is measured in."
        metadata_unit.short_name = "unit"
        metadata_unit.file_extension = "json"
        log.info("Saving unit descriptions...")
        unit_file = os.path.join(temp_dir, f"data.{metadata_unit.file_extension}")
        with open(unit_file, "w") as fp:
            json.dump(unit_desc, fp)

        log.info("Adding unit descriptions to catalog...")
        add_to_catalog(metadata_unit, unit_file, upload=True)  # type: ignore

        log.info("Downloading dimension descriptions...")
        dim_desc = dimensions_description()
        metadata_dim = metadata
        metadata_dim.description = "A description of the dimensions of the data."
        metadata_dim.short_name = "dimension"
        metadata_dim.file_extension = "json"
        log.info("Saving dimension descriptions...")
        dim_file = os.path.join(temp_dir, f"data.{metadata_dim.file_extension}")
        with open(dim_file, "w") as fp:
            json.dump(dim_desc, fp)

        log.info("Adding dimension descriptions to catalog...")
        add_to_catalog(metadata_dim, dim_file, upload=True)  # type: ignore


def create_metadata():
    meta = load_yaml_metadata()
    meta.update(load_external_metadata())

    return Dataset(
        **meta,
        date_accessed=dt.datetime.now().date(),
    )


def load_yaml_metadata() -> dict:
    fpath = Path(__file__).parent / f"{Path(__file__).stem}.meta.yml"
    with open(fpath) as istream:
        meta = yaml.safe_load(istream)
    return meta


def load_external_metadata() -> dict:
    meta_orig = pd.read_excel(URL_METADATA)
    meta_orig.columns = ["updated", "detail"]
    pub_date = meta_orig["detail"].iloc[0].date()

    meta = {
        "name": f"United Nations Sustainable Development Goals - United Nations ({pub_date})",
        "publication_year": pub_date.year,
        "publication_date": f"{pub_date}",
    }
    return meta


def download_data() -> pd.DataFrame:
    # retrieves all goal codes
    print("Retrieving SDG goal codes...")
    url = f"{BASE_URL}/v1/sdg/Goal/List"
    res = requests.get(url)
    assert res.ok

    goals = res.json()
    goal_codes = [str(goal["code"]) for goal in goals]

    # retrieves all area codes
    print("Retrieving area codes...")
    url = f"{BASE_URL}/v1/sdg/GeoArea/List"
    res = requests.get(url)
    assert res.ok
    areas = res.json()
    area_codes = [str(area["geoAreaCode"]) for area in areas]
    # retrieves csv with data for all codes and areas
    print("Retrieving data...")
    url = f"{BASE_URL}/v1/sdg/Goal/DataCSV"
    all_data = []
    for goal in goal_codes:
        content = download_file(url=url, goal=goal, area_codes=area_codes, max_retries=MAX_RETRIES)
        df = pd.read_csv(BytesIO(content), low_memory=False)
        all_data.append(df)
    all_df = pd.concat(all_data)
    all_df = all_df.reset_index()
    cols = all_df.columns
    # Converting all columns to string dtype as feather doesn't like object dtype
    all_df[cols] = all_df[cols].astype("str")

    return all_df


def download_file(url: str, goal: int, area_codes: list, max_retries: int, bytes_read: int = 0) -> bytes:
    """Downloads a file from a url.

    Retries download up to {max_retries} times following a ChunkedEncodingError
    exception.
    """
    log.info(
        "Downloading data...",
        url=url,
        bytes_read=bytes_read,
        remaining_retries=max_retries,
        goal=goal,
    )
    if bytes_read:
        headers = {"Range": f"bytes={bytes_read}-"}
    else:
        headers = {}

    content = b""
    try:
        with requests.post(
            url,
            data={"goal": goal, "areaCodes": area_codes},
            headers=headers,
            stream=True,
        ) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                bytes_read += CHUNK_SIZE
                content += chunk
    except requests.exceptions.ChunkedEncodingError:
        if max_retries > 0:
            log.info("Encountered ChunkedEncodingError, resuming download...")
            content += download_file(
                url=url,
                goal=goal,
                area_codes=area_codes,
                max_retries=max_retries - 1,
                bytes_read=bytes_read,
            )
        else:
            log.error(
                "Encountered ChunkedEncodingError, but max_retries has been "
                "exceeded. Download may not have been fully completed."
            )
    return content


def attributes_description() -> Dict[Any, Any]:
    goal_codes = get_goal_codes()
    a = []
    for goal in goal_codes:
        url = f"{BASE_URL}/v1/sdg/Goal/{goal}/Attributes"
        res = requests.get(url)
        assert res.ok
        attr = res.json()
        for att in attr:
            for code in att["codes"]:
                a.append(
                    {
                        "code": code["code"],
                        "description": code["description"],
                    }
                )
    att_dict = pd.DataFrame(a).drop_duplicates().set_index("code").squeeze().to_dict()
    att_dict["PERCENT"] = "%"
    return att_dict


def dimensions_description() -> dict:
    goal_codes = get_goal_codes()
    d = []
    for goal in goal_codes:
        url = f"{BASE_URL}/v1/sdg/Goal/{goal}/Dimensions"
        res = requests.get(url)
        assert res.ok
        dims = res.json()
        for dim in dims:
            for code in dim["codes"]:
                d.append(
                    {
                        "id": dim["id"],
                        "code": code["code"],
                        "description": code["description"],
                    }
                )
    dim_dict = defaultdict(lambda: {np.nan: ""})
    for dimen in d:
        dim_dict[dimen["id"]][dimen["code"]] = dimen["description"]

    return dim_dict


def get_goal_codes() -> List[int]:
    # retrieves all goal codes
    url = f"{BASE_URL}/v1/sdg/Goal/List"
    res = requests.get(url)
    assert res.ok
    goals = res.json()
    goal_codes = [int(goal["code"]) for goal in goals]
    return goal_codes


if __name__ == "__main__":
    main()

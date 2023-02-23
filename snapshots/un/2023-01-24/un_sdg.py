"""Script to create a snapshot of dataset 'United Nations Sustainable Development Goals (2023)'.
    As well as a snapshot of the data we collect a snapshot of the dimensions and attributes of the data.
    These often change as the dataset contains many different variables with many different dimensions and values/attributes.
"""
import datetime as dt
import json
import os
import tempfile
from collections import defaultdict
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List

import click
import numpy as np
import pandas as pd
import requests
import yaml
from structlog import get_logger

from etl.snapshot import Snapshot, SnapshotMeta, add_snapshot

log = get_logger()


URL_METADATA = "https://unstats.un.org/sdgs/indicators/SDG_Updateinfo.xlsx"
MAX_RETRIES = 10
CHUNK_SIZE = 1024 * 1024 * 10


# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"un/{SNAPSHOT_VERSION}/un_sdg.feather")

    # Download data from source.

    log.info("Creating metadata...")
    metadata = create_metadata(snap)
    with tempfile.TemporaryDirectory() as temp_dir:
        log.info("Downloading unit descriptions...")
        unit_desc = attributes_description(snap)
        unit_desc = pd.DataFrame(unit_desc.items(), columns=["AttCode", "AttValue"])
        log.info("Adding unit descriptions to catalog...")
        add_snapshot("un/2023-01-24/un_sdg_unit.csv", dataframe=unit_desc, upload=upload)

        log.info("Downloading dimension descriptions...")
        dim_desc = dimensions_description(snap)
        dim_file = os.path.join(temp_dir, "data.json")
        with open(dim_file, "w") as fp:
            json.dump(dim_desc, fp)

        log.info("Adding dimension descriptions to catalog...")
        add_snapshot("un/2023-01-24/un_sdg_dimension.json", filename=dim_file, upload=upload)  # type: ignore

        # fetch the file locally
        assert metadata.source_data_url is not None
        log.info("Downloading data...")
        all_data = download_data(snap)
        log.info("Adding data to catalog...")
        add_snapshot("un/2023-01-24/un_sdg.feather", dataframe=all_data, upload=upload)


def create_metadata(snap: Snapshot) -> SnapshotMeta:
    """Updating metadata in so it matches the UN SDG update log"""
    meta = snap.metadata
    meta_update = load_external_metadata()
    meta.name = meta_update["name"]
    meta.publication_year = meta_update["publication_year"]
    meta.publication_date = meta_update["publication_date"]
    meta.date_accessed = dt.datetime.now().date()
    return meta


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


def download_data(snap: Snapshot) -> pd.DataFrame:
    # retrieves all goal codes
    print("Retrieving SDG goal codes...")
    url = f"{snap.metadata.source_data_url}/v1/sdg/Goal/List"
    res = requests.get(url)
    assert res.ok

    goals = res.json()
    goal_codes = [str(goal["code"]) for goal in goals]

    # retrieves all area codes
    print("Retrieving area codes...")
    url = f"{snap.metadata.source_data_url}/v1/sdg/GeoArea/List"
    res = requests.get(url)
    assert res.ok
    areas = res.json()
    area_codes = [str(area["geoAreaCode"]) for area in areas]
    # retrieves csv with data for all codes and areas
    print("Retrieving data...")
    url = f"{snap.metadata.source_data_url}/v1/sdg/Goal/DataCSV"
    all_data = []
    for goal in goal_codes:
        content = download_file(url=url, goal=goal, area_codes=area_codes, max_retries=MAX_RETRIES)
        s = str(content, "utf-8")
        data = StringIO(s)
        df = pd.read_csv(data, low_memory=False)
        # df = pd.read_csv(BytesIO(content), engine="python")
        all_data.append(df)
    all_df = pd.concat(all_data)
    all_df = all_df.reset_index()
    cols = all_df.columns
    # Converting all columns to string dtype as feather doesn't like object dtype
    all_df[cols] = all_df[cols].astype("str")
    all_df = pd.DataFrame(all_df)

    return all_df


def download_file(url: str, goal: str, area_codes: list, max_retries: int, bytes_read: int = 0) -> bytes:
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


def attributes_description(snap: Snapshot) -> Dict[Any, Any]:
    """Gathers each of the unit codes and their more descriptive counterparts."""
    goal_codes = get_goal_codes(snap)
    a = []
    for goal in goal_codes:
        url = f"{snap.metadata.source_data_url}/v1/sdg/Goal/{goal}/Attributes"
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


def dimensions_description(snap: Snapshot) -> dict:
    """Gathers each of the dimension codes and their more descriptive versions. This updates regularly so is important to snapshot"""
    goal_codes = get_goal_codes(snap)
    d = []
    for goal in goal_codes:
        url = f"{snap.metadata.source_data_url}/v1/sdg/Goal/{goal}/Dimensions"
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


def get_goal_codes(snap: Snapshot) -> List[int]:

    # retrieves all goal codes
    url = f"{snap.metadata.source_data_url}/v1/sdg/Goal/List"
    res = requests.get(url)
    assert res.ok
    goals = res.json()
    goal_codes = [int(goal["code"]) for goal in goals]
    return goal_codes


if __name__ == "__main__":
    main()

import gzip
import json
import random
from pathlib import Path
from typing import List

import click
import pandas as pd
import structlog
from deepdiff import DeepDiff

from etl.db import get_engine

log = structlog.get_logger()

engine = get_engine()


@click.command()
@click.argument("s3_path")
@click.argument("baked_path")
@click.option("--variables", "-v", type=int, multiple=True)
def data_parity_cli(s3_path: Path, baked_path: Path, variables: List[int]) -> None:
    """Compare two sets of generated data files, typically from S3 and baked files from live
    server. Find all differences between the two sets.

    First sync data from S3:

        aws s3 sync s3://owid-catalog/baked-variables/live_grapher/ playground/s3-live/

    and sync baked data from live

        rsync -vr --progress --delete owid@owid-live:/home/owid/live/bakedSite/grapher/data/variables/ playground/baked-live/

    Then run this script:

        ENV=.env.prod python scripts/data_parity.py playground/s3-live/ playground/baked-live/

    I've been having trouble with `aws s3 sync` not syncing refreshed files, so it might be safer to redownload the
    entire directory or at least problematic variables.
    """
    s3_path = Path(s3_path)
    baked_path = Path(baked_path)

    if variables:
        vars_in_charts = variables
    else:
        vars_in_charts = _variables_in_charts()

    random.shuffle(vars_in_charts)

    for var_id in vars_in_charts:
        _compare_data(baked_path, s3_path, var_id)
        _compare_metadata(baked_path, s3_path, var_id)


def _compare_metadata(baked_path: Path, s3_path: Path, var_id: int) -> None:
    # read live site
    p = Path(baked_path / f"metadata/{var_id}.json")
    try:
        with open(p) as f:
            live_data = _parse_metadata(f.read())
    except FileNotFoundError:
        log.warning("File not found on live", path=p)
        return

    # read s3
    p = Path(s3_path / f"metadata/{var_id}.json")
    try:
        with gzip.open(p, "rb") as f:
            raw = f.read().decode()
            s3_data = _parse_metadata(raw)
    except FileNotFoundError:
        log.warning("File not found on S3", path=p)
        return

    # remove dimensions for now
    s3_data.pop("dimensions")
    live_data.pop("dimensions")

    if s3_data != live_data:
        diff = DeepDiff(s3_data, live_data)
        log.warning("S3 and live have different metadata", var_id=var_id, dataset_id=live_data["datasetId"], diff=diff)


def _compare_data(baked_path: Path, s3_path: Path, var_id: int) -> None:
    # read live site
    p = Path(baked_path / f"data/{var_id}.json")
    try:
        with open(p) as f:
            live_data = _parse_data(f.read())
    except FileNotFoundError:
        log.warning("File not found on live", path=p)
        return

    # read s3
    p = Path(s3_path / f"data/{var_id}.json")
    try:
        with gzip.open(p, "rb") as f:
            raw = f.read().decode()
            s3_data = _parse_data(raw)
    except FileNotFoundError:
        log.warning("File not found on S3", path=p)
        return

    if s3_data != live_data:
        if len(s3_data) != len(live_data):
            log.warning(
                "S3 and live have different length", s3_len=len(s3_data), live_len=len(live_data), var_id=var_id
            )
        else:
            # iterate through all values
            for i in range(len(s3_data)):
                if s3_data[i] != live_data[i]:
                    log.warning("S3 and live have different values", s3=s3_data[i], live=live_data[i], var_id=var_id)
                    break


def _parse_data(js):
    x = json.loads(js)
    return sorted(zip(x["entities"], x["years"], x["values"]))


def _parse_metadata(js):
    x = json.loads(js)

    # type on live is not computed correctly, but we're not using it anyway
    x.pop("type")

    # does not have to match
    x.pop("updatedAt")
    x.pop("dataPath", None)
    x.pop("metadataPath", None)

    return x


def _variables_in_charts() -> List[int]:
    q = """
    select distinct vars.varID as varId
    from charts c, json_table(c.config, '$.dimensions[*]' columns (varID integer path '$.variableId') ) as vars
    where JSON_EXTRACT(c.config, '$.isPublished')=true;
    """
    df = pd.read_sql(q, engine)
    return df["varId"].tolist()


if __name__ == "__main__":
    data_parity_cli()

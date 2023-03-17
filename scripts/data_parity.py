import gzip
import json
from pathlib import Path
from typing import List

import click
import pandas as pd
import structlog

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
    """
    s3_path = Path(s3_path)
    baked_path = Path(baked_path)

    if variables:
        vars_in_charts = variables
    else:
        vars_in_charts = _variables_in_charts()

    for var_id in vars_in_charts:
        p = Path(baked_path / f"data/{var_id}.json")

        # read live site
        try:
            with open(p) as f:
                live = _parse(f.read())
        except FileNotFoundError:
            log.warning("File not found on live", path=p)
            continue

        # read s3
        p = Path(s3_path / f"data/{var_id}.json")
        try:
            with gzip.open(p, "rb") as f:
                raw = f.read().decode()
                s3 = _parse(raw)
        except FileNotFoundError:
            log.warning("File not found on S3", path=p)
            continue

        if s3 != live:
            if len(s3) != len(live):
                log.warning("S3 and live have different length", s3_len=len(s3), live_len=len(live), var_id=var_id)
            else:
                # iterate through all values
                for i in range(len(s3)):
                    if s3[i] != live[i]:
                        log.warning("S3 and live have different values", s3=s3[i], live=live[i], var_id=var_id)
                        break


def _parse(js):
    x = json.loads(js)
    return sorted(zip(x["entities"], x["years"], x["values"]))


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

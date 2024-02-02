import concurrent.futures
import gzip
import json
from io import BytesIO

import pandas as pd
import rich_click as click
import structlog

from apps.backport.datasync.datasync import upload_gzip_dict
from etl.db import get_engine
from etl.publish import connect_s3

log = structlog.get_logger()

RENAME_MAP = {
    "Timor": "East Timor",
    "Saint Barthélemy": "Saint Barthelemy",
    "Åland Islands": "Aland Islands",
    "Faeroe Islands": "Faroe Islands",
    "Eritrea and Ethiopia": "Ethiopia (former)",
    "United Korea": "Korea (former)",
}


def update_file(s3, bucket, key, dry_run):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except s3.exceptions.NoSuchKey:
        log.warning("update_file.missing_file", key=key)
        return

    content = response["Body"].read()

    if content.startswith(b"{"):
        decompressed_content = content
    else:
        with gzip.open(BytesIO(content), "rb") as f:
            decompressed_content = f.read()

    data = json.loads(decompressed_content)

    renamed = False
    for entity_dict in data["dimensions"]["entities"]["values"]:
        for old_name, new_name in RENAME_MAP.items():
            if entity_dict["name"] == old_name:
                entity_dict["name"] = new_name
                renamed = True
                log.info("update_file.rename", key=key, old_name=old_name, new_name=new_name)

    if renamed and not dry_run:
        s3_path = f"s3://{bucket}/{key}"
        upload_gzip_dict(data, s3_path)
        log.info("update_file.upload", s3_path=s3_path)


def list_files(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for content in page.get("Contents", []):
            yield content["Key"]


def list_files_from_variable_ids(variable_ids, prefix):
    for var_id in variable_ids:
        yield f"{prefix}{var_id}.metadata.json"


def load_variable_ids(limit) -> list[int]:
    q = f"""
    select
        v.id
    from variables as v
    join datasets as d on v.datasetId = d.id
    where d.isArchived = 0
    order by rand()
    limit {limit}
    """
    df = pd.read_sql(q, get_engine())
    return list(df["id"])


@click.command(help=__doc__)
@click.argument(
    "bucket",
    type=str,
    # help="Bucket name, e.g. owid-catalog",
)
@click.argument(
    "prefix",
    type=str,
    # help="Prefix, e.g. baked-variables/staging_grapher/metadata/",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    type=bool,
    help="Do not modify file on S3",
)
@click.option(
    "--workers",
    type=int,
    help="Thread workers to parallelize",
    default=1,
)
@click.option(
    "--limit",
    type=int,
    help="Max number of variables to check",
    default=1000000,
)
def cli(
    bucket: str,
    prefix: str,
    dry_run: bool,
    workers: int,
    limit: int,
) -> None:
    """
    Iterate over all files in s3://BUCKET/PREFIX and rename entities in the entities dimension.

    Example:
        ENV=.env.prod python scripts/audit_s3.py owid-api v1/indicators/ --dry-run --workers 10 --limit 1000
    """
    assert dry_run, "Only --dry-run is supported at the moment, we don't want to modify files on S3"

    s3 = connect_s3()

    variable_ids = load_variable_ids(limit)

    keys = list(list_files_from_variable_ids(variable_ids, prefix))

    log.info("cli", files_count=len(keys))

    if workers == 1:
        for key in keys:
            update_file(s3, bucket, key, dry_run)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            executor.map(lambda key: update_file(s3, bucket, key, dry_run), keys)


if __name__ == "__main__":
    cli()

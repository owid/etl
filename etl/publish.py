#
#  publish.py
#  etl
#

import sys
from typing import Any, Dict, List, Optional
from urllib.request import HTTPError
from pathlib import Path

import click
import boto3
from botocore.client import ClientError
import pandas as pd

from owid.catalog import LocalCatalog

from etl import config, files
from etl.command import DATA_DIR


class CannotPublish(Exception):
    pass


@click.command()
@click.option("--dry-run", is_flag=True)
def publish(dry_run: bool = False) -> None:
    """
    Publish the generated data catalog to S3.
    """
    sanity_checks()
    sync_catalog_to_s3(dry_run=dry_run)


def sanity_checks() -> None:
    if not (DATA_DIR / "catalog.feather").exists():
        print(
            "ERROR: catalog has not been indexed, refusing to publish", file=sys.stderr
        )
        sys.exit(1)


def sync_catalog_to_s3(dry_run: bool = False) -> None:
    s3 = connect_s3()
    if is_catalog_up_to_date(s3):
        print("Catalog is up to date!")
        return

    sync_datasets(s3, dry_run=dry_run)
    if not dry_run:
        update_catalog(s3)


def is_catalog_up_to_date(s3: Any) -> bool:
    remote = get_remote_checksum(s3, "catalog.feather")
    local = files.checksum_file(DATA_DIR / "catalog.feather")
    return remote == local


def sync_datasets(s3, dry_run: bool = False):
    "Go dataset by dataset and check if each one needs updating."
    existing = get_published_checksums()

    to_delete = set(existing)
    local = LocalCatalog(DATA_DIR)
    print("Datasets to sync:")
    for ds in local.iter_datasets():
        path = Path(ds.path).relative_to(DATA_DIR).as_posix()
        if path in to_delete:
            to_delete.remove(path)

        published_checksum = existing.get(path)
        if published_checksum == ds.checksum():
            continue

        print("-", path)
        if not dry_run:
            sync_folder(s3, DATA_DIR / path, path)

    print("Datasets to delete:")
    for path in to_delete:
        print("-", path)
        if not dry_run:
            delete_dataset(s3, path)


def sync_folder(s3: Any, local_folder, dest_path: str, delete: bool = True) -> None:
    """
    Perform a content-based sync of a local folder with a "folder" on an S3 bucket,
    by comparing checksums and only uploading files that have changed.
    """
    existing = {
        o["Key"]: object_md5(o) for o in walk_s3(s3, config.S3_BUCKET, dest_path)
    }

    for filename in files.walk(local_folder):
        checksum = files.checksum_file(filename)
        rel_filename = filename.relative_to(DATA_DIR).as_posix()
        if checksum != existing.get(rel_filename):
            print("  PUT", rel_filename)
            s3.upload_file(
                filename.as_posix(),
                config.S3_BUCKET,
                rel_filename,
                ExtraArgs={"ACL": "public-read"},
            )

        del existing[rel_filename]

    if delete:
        for rel_filename in existing:
            print("  DEL", rel_filename)


def object_md5(obj: dict) -> str:
    return obj["ETag"].strip("'\"")


def walk_s3(s3: Any, bucket: str, path: str) -> List[dict]:
    objs = s3.list_objects(Bucket=bucket, Prefix=path)
    yield from objs["Contents"]

    while objs["IsTruncated"]:
        objs = s3.list_objects(Bucket=bucket, Prefix=path, Marker=objs["NextMarker"])
        yield from objs["Contents"]


def delete_dataset(s3: Any, path: Path) -> None:
    relative_path = path.relative_to(DATA_DIR).as_posix()
    to_delete = [o["Key"] for o in walk_s3(s3, config.S3_BUCKET, relative_path)]
    while to_delete:
        chunk = to_delete[:1000]
        s3.delete_objects(
            Bucket=config.S3_BUCKET,
            Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
        )
        to_delete = to_delete[1000:]


def update_catalog(s3: Any) -> None:
    s3.upload_file(
        (DATA_DIR / "catalog.feather").as_posix(),
        config.S3_BUCKET,
        "catalog.feather",
        ExtraArgs={"ACL": "public-read"},
    )


def get_published_checksums() -> Dict[str, str]:
    "Get the checksum of every dataset that's been published."
    try:
        existing = pd.read_feather(
            f"https://{config.S3_BUCKET}.{config.S3_HOST}/catalog.feather"
        )
        existing["path"] = existing["path"].apply(lambda p: p.rsplit("/", 1)[0])
        existing = (
            existing[["path", "checksum"]]
            .drop_duplicates()
            .set_index("path")
            .checksum.to_dict()
        )
    except HTTPError:
        existing = {}

    return existing


def get_remote_checksum(s3: Any, path: str) -> Optional[str]:
    try:
        obj = s3.head_object(Bucket=config.S3_BUCKET, Key=path)
    except ClientError:
        if "Not Found" in ClientError.args[0]:
            return None

        raise

    return object_md5(obj)


def connect_s3() -> Any:
    session = boto3.Session()
    return session.client(
        "s3",
        region_name=config.S3_REGION_NAME,
        endpoint_url=config.S3_ENDPOINT_URL,
        aws_access_key_id=config.S3_ACCESS_KEY,
        aws_secret_access_key=config.S3_SECRET_KEY,
    )


if __name__ == "__main__":
    publish()

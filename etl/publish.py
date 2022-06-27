#
#  publish.py
#  etl
#

import re
import sys
from typing import Any, Dict, Iterator, Optional, cast
from collections.abc import Iterable
from urllib.error import HTTPError
from pathlib import Path
import concurrent.futures

import click
import boto3
from botocore.client import ClientError
import pandas as pd

from owid.catalog import LocalCatalog, CHANNEL

from etl import config, files
from etl.paths import DATA_DIR


class CannotPublish(Exception):
    pass


@click.command()
@click.option("--dry-run", is_flag=True, default=False)
@click.option("--private", is_flag=True, default=False)
@click.option("--bucket", type=str, help="Bucket name", default=config.S3_BUCKET)
@click.option(
    "--channel",
    "-c",
    multiple=True,
    type=click.Choice(CHANNEL.__args__),
    default=CHANNEL.__args__,
    help="Publish only selected channel (subfolder of data/), push all by default",
)
def publish_cli(
    dry_run: bool, private: bool, bucket: str, channel: Iterable[CHANNEL]
) -> None:
    """
    Publish the generated data catalog to S3.
    """
    return publish(
        dry_run=dry_run,
        private=private,
        bucket=bucket,
        channel=channel,
    )


def publish(
    dry_run: bool = False,
    private: bool = False,
    bucket: str = config.S3_BUCKET,
    channel: Iterable[CHANNEL] = CHANNEL.__args__,
) -> None:
    catalog = Path(DATA_DIR)
    if not dry_run and not private:
        raise Exception(
            "You cannot publish public catalog yet, only private catalogs with flag --private are supported"
        )
    for c in channel:
        sanity_checks(catalog, channel=c)

    for c in channel:
        sync_catalog_to_s3(bucket, catalog, channel=c, dry_run=dry_run)


def sanity_checks(catalog: Path, channel: CHANNEL) -> None:
    if not (catalog / _channel_path(channel)).exists():
        print(
            "ERROR: catalog has not been indexed, refusing to publish", file=sys.stderr
        )
        sys.exit(1)


def sync_catalog_to_s3(
    bucket: str, catalog: Path, channel: CHANNEL, dry_run: bool = False
) -> None:
    s3 = connect_s3()
    if is_catalog_up_to_date(s3, bucket, catalog, channel):
        print(f"Catalog's channel {channel} is up to date!")
        return

    print(f"Syncing datasets from channel {channel}")
    sync_datasets(s3, bucket, catalog, channel, dry_run=dry_run)
    if not dry_run:
        update_catalog(s3, bucket, catalog, channel)


def is_catalog_up_to_date(
    s3: Any, bucket: str, catalog: Path, channel: CHANNEL
) -> bool:
    """The catalog file is synced last -- if it is the same as our local one, then all the remote
    files will be the same as our local ones too."""
    remote = get_remote_checksum(s3, bucket, _channel_path(channel).as_posix())
    local = files.checksum_file(catalog / _channel_path(channel))
    return remote == local


def sync_datasets(
    s3: Any, bucket: str, catalog: Path, channel: CHANNEL, dry_run: bool = False
) -> None:
    "Go dataset by dataset and check if each one needs updating."
    existing = get_published_checksums(bucket, channel)

    to_delete = set(existing)
    local = LocalCatalog(catalog)
    print("Datasets to sync:")
    for ds in local.iter_datasets(channel):
        # ignore datasets with no tables
        if len(ds._data_files) == 0:
            continue

        path = Path(ds.path).relative_to(catalog).as_posix()
        if path in to_delete:
            to_delete.remove(path)

        published_checksum = existing.get(path)
        if published_checksum == ds.checksum():
            continue

        print("-", path)
        if not dry_run:
            sync_folder(
                s3, bucket, catalog, catalog / path, path, public=ds.metadata.is_public
            )

    print("Datasets to delete:")
    for path in to_delete:
        print("-", path)
        if not dry_run:
            delete_dataset(s3, bucket, path)


def sync_folder(
    s3: Any,
    bucket: str,
    catalog: Path,
    local_folder: Path,
    dest_path: str,
    delete: bool = True,
    public: bool = True,
) -> None:
    """
    Perform a content-based sync of a local folder with a "folder" on an S3 bucket,
    by comparing checksums and only uploading files that have changed.
    """
    existing = {
        o["Key"]: object_md5(s3, bucket, o["Key"], o)
        for o in walk_s3(s3, bucket, dest_path)
    }

    # some datasets like `open_numbers/open_numbers/latest/gapminder__gapminder_world`
    # have huge number of tables, upload them in parallel
    futures = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for filename in files.walk(local_folder):
            checksum = files.checksum_file(filename)
            rel_filename = filename.relative_to(catalog).as_posix()

            existing_checksum = existing.get(rel_filename)

            if checksum != existing_checksum:
                print("  PUT", rel_filename)
                ExtraArgs: Dict[str, Any] = {"Metadata": {"md5": checksum}}
                if public:
                    ExtraArgs["ACL"] = "public-read"
                futures.append(
                    executor.submit(
                        s3.upload_file,
                        filename.as_posix(),
                        bucket,
                        rel_filename,
                        ExtraArgs=ExtraArgs,
                    )
                )

            if rel_filename in existing:
                del existing[rel_filename]

        if delete:
            for rel_filename in existing:
                print("  DEL", rel_filename)
                futures.append(
                    executor.submit(s3.delete_object, Bucket=bucket, Key=rel_filename)
                )

        concurrent.futures.wait(futures)


def object_md5(s3: Any, bucket: str, key: str, obj: Dict[str, Any]) -> Optional[str]:
    maybe_md5 = obj["ETag"].strip('"')
    if re.match("^[0-9a-f]{32}$", maybe_md5):
        return cast(str, maybe_md5)

    return cast(
        Optional[str],
        s3.head_object(Bucket=bucket, Key=key).get("Metadata", {}).get("md5"),
    )


def walk_s3(s3: Any, bucket: str, path: str) -> Iterator[Dict[str, Any]]:
    objs = s3.list_objects(Bucket=bucket, Prefix=path)
    yield from objs.get("Contents", [])

    while objs["IsTruncated"]:
        objs = s3.list_objects(Bucket=bucket, Prefix=path, Marker=objs["NextMarker"])
        yield from objs.get("Contents", [])


def delete_dataset(s3: Any, bucket: str, relative_path: str) -> None:
    to_delete = [o["Key"] for o in walk_s3(s3, bucket, relative_path)]
    while to_delete:
        chunk = to_delete[:1000]
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
        )
        to_delete = to_delete[1000:]


def update_catalog(s3: Any, bucket: str, catalog: Path, channel: CHANNEL) -> None:
    catalog_filename = catalog / _channel_path(channel)
    s3.upload_file(
        catalog_filename.as_posix(),
        bucket,
        _channel_path(channel).as_posix(),
        ExtraArgs={"ACL": "public-read"},
    )

    s3.upload_file(
        (catalog / "catalog.meta.json").as_posix(),
        bucket,
        "catalog.meta.json",
        ExtraArgs={"ACL": "public-read"},
    )


def get_published_checksums(bucket: str, channel: CHANNEL) -> Dict[str, str]:
    "Get the checksum of every dataset that's been published."
    try:
        existing = pd.read_feather(
            f"https://{bucket}.{config.S3_HOST}/{_channel_path(channel)}"
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

    return cast(Dict[str, str], existing)


def get_remote_checksum(s3: Any, bucket: str, path: str) -> Optional[str]:
    try:
        obj = s3.head_object(Bucket=bucket, Key=path)
    except ClientError as e:
        if "Not Found" in e.args[0]:
            return None

        raise

    return object_md5(s3, bucket, path, obj)


def connect_s3() -> Any:
    # TODO: use https://github.com/owid/data-utils-py/blob/main/owid/datautils/io/s3.py
    session = boto3.Session()
    return session.client(
        "s3",
        region_name=config.S3_REGION_NAME,
        endpoint_url=config.S3_ENDPOINT_URL,
        aws_access_key_id=config.S3_ACCESS_KEY,
        aws_secret_access_key=config.S3_SECRET_KEY,
    )


def _channel_path(channel: CHANNEL) -> Path:
    return Path(f"catalog-{channel}.feather")


if __name__ == "__main__":
    publish_cli()

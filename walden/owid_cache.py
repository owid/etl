#
#  owid_cache.py
#
#  Helpers for working with our cache in DigitalOcean Spaces.
#

import os
from os import path
import logging
from typing import Optional, Tuple
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from owid.walden.ui import log, bail
from .files import checksum, ChecksumDoesNotMatch

SPACES_ENDPOINT = "https://nyc3.digitaloceanspaces.com"
S3_BASE = "s3://walden.nyc3.digitaloceanspaces.com"
HTTPS_BASE = "https://walden.nyc3.digitaloceanspaces.com"
AWS_PROFILE = os.environ.get("AWS_PROFILE", "default")


def upload(filename: str, relative_path: str, public: bool = False) -> str:
    """
    Upload file to Walden.

    Args:
        local_path (str): Local path to file.
        walden_path (str): Path where to store the file in Walden.
        public (bool): Set to True to expose the file to the public (read only). Defaults to False.
    """
    dest_path = f"{S3_BASE}/{relative_path}"
    extra_args = {"ACL": "public-read"} if public else {}

    client = connect()
    try:
        client.upload_file(filename, "walden", relative_path, ExtraArgs=extra_args)
    except ClientError as e:
        logging.error(e)
        raise UploadError(e)

    log("UPLOADED", f"{filename} -> {dest_path}")

    return f"{HTTPS_BASE}/{relative_path}"


def s3_bucket_key(url: str) -> Tuple[str, str]:
    """Get bucket and key from either s3:// URL or https:// URL."""
    parsed = urlparse(url)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    # strip region from bucket name in https scheme
    if parsed.scheme == "https":
        bucket = bucket.split(".")[0]

    return bucket, key


def download(
    s3_url: str, filename: str, expected_md5: Optional[str] = None, quiet: bool = False
) -> None:
    """Download the file at the S3 URL to the given local filename."""
    client = connect()

    bucket, key = s3_bucket_key(s3_url)

    try:
        client.download_file(bucket, key, filename)
    except ClientError as e:
        logging.error(e)
        raise UploadError(e)

    if expected_md5:
        if checksum(filename) != expected_md5:
            os.remove(filename)
            raise ChecksumDoesNotMatch(f"for file downloaded from {s3_url}")

    if not quiet:
        log("DOWNLOADED", f"{s3_url} -> {filename}")


def connect():
    "Return a connection to Walden's DigitalOcean space."
    check_for_default_profile()

    session = boto3.Session(profile_name=AWS_PROFILE)
    client = session.client(
        service_name="s3",
        endpoint_url=SPACES_ENDPOINT,
    )
    return client


def check_for_default_profile():
    filename = path.expanduser("~/.aws/config")
    if not path.exists(filename) or f"[{AWS_PROFILE}]" not in open(filename).read():
        bail(
            f"""you must set up a config file at ~/.aws/config

it should look like:

[{AWS_PROFILE}]
aws_access_key_id = ...
aws_secret_access_key = ...
"""
        )


class UploadError(Exception):
    pass

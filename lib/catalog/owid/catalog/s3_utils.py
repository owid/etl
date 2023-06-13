"""There's a significant overlap with https://github.com/owid/walden/blob/master/owid/walden/owid_cache.py
It would make sense to move both into a shared module in the future or use some proper public library
for working with S3 that is compatible with DigitalOcean's Spaces.
"""
import logging
import os
from os import path
from typing import Any, Tuple
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

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

    logging.info("UPLOADED", f"{filename} -> {dest_path}")

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


def download(s3_url: str, filename: str, quiet: bool = False) -> None:
    """Download the file at the S3 URL to the given local filename."""
    client = connect()

    bucket, key = s3_bucket_key(s3_url)

    try:
        client.download_file(bucket, key, filename)
    except ClientError as e:
        logging.error(e)
        raise UploadError(e)

    if not quiet:
        logging.info("DOWNLOADED", f"{s3_url} -> {filename}")


def connect() -> Any:
    "Return a connection to Walden's DigitalOcean space."
    check_for_default_profile()

    session = boto3.Session(profile_name=AWS_PROFILE)
    client = session.client(
        service_name="s3",
        endpoint_url=SPACES_ENDPOINT,
    )
    return client


def check_for_default_profile() -> None:
    filename = path.expanduser("~/.aws/config")
    if not path.exists(filename) or f"[{AWS_PROFILE}]" not in open(filename).read():
        raise MissingCredentialsError(
            f"""you must set up a config file at ~/.aws/config

it should look like:

[{AWS_PROFILE}]
aws_access_key_id = ...
aws_secret_access_key = ...
region = ...
"""
        )


class MissingCredentialsError(Exception):
    pass


class UploadError(Exception):
    pass

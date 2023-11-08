import logging
import os
from os import path
from typing import Any, Tuple
from urllib.parse import urlparse

from botocore.exceptions import ClientError

R2_ENDPOINT = "https://078fcdfed9955087315dd86792e71a7e.r2.cloudflarestorage.com"
AWS_PROFILE = os.environ.get("AWS_PROFILE", "default")


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
    "Return a connection to Cloudflare's R2."
    import boto3

    check_for_default_profile()

    session = boto3.Session(profile_name=AWS_PROFILE)
    client = session.client(
        service_name="s3",
        endpoint_url=R2_ENDPOINT,
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

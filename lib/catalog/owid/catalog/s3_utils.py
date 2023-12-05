import logging
from os import environ as env
from typing import Any, Tuple
from urllib.parse import urlparse

from botocore.exceptions import ClientError

R2_ENDPOINT = "https://078fcdfed9955087315dd86792e71a7e.r2.cloudflarestorage.com"


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

    R2_ACCESS_KEY = env.get("R2_ACCESS_KEY")
    R2_SECRET_KEY = env.get("R2_SECRET_KEY")

    assert R2_ACCESS_KEY and R2_SECRET_KEY, "Missing R2_ACCESS_KEY and R2_SECRET_KEY environment variables."

    client = boto3.client(
        service_name="s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
    )

    return client


class MissingCredentialsError(Exception):
    pass


class UploadError(Exception):
    pass

import concurrent.futures
import configparser
import os
import threading
from functools import lru_cache
from os import environ as env
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

# import botocore.client.S3 as BaseClient
import structlog
from botocore.client import BaseClient
from botocore.exceptions import ClientError

log = structlog.get_logger()

BOTO3_CLIENT_LOCK = threading.Lock()


def s3_bucket_key(url: str) -> Tuple[str, str]:
    """Get bucket and key from either s3:// URL or https:// URL."""
    parsed = urlparse(url)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    # strip region from bucket name in https scheme
    if parsed.scheme == "https":
        bucket = bucket.split(".")[0]

    return bucket, key


def list_s3_objects(s3_folder: str, client: Optional[BaseClient] = None) -> List[str]:
    client = client or connect_r2()

    bucket, key = s3_bucket_key(s3_folder)
    continuation_token = None
    keys = []

    while True:
        if continuation_token:
            response = client.list_objects_v2(Bucket=bucket, Prefix=key, ContinuationToken=continuation_token)  # type: ignore
        else:
            response = client.list_objects_v2(Bucket=bucket, Prefix=key)  # type: ignore

        if "Contents" in response:
            keys.extend([obj["Key"] for obj in response["Contents"] if not obj["Key"].endswith("/")])

        if response.get("IsTruncated"):
            continuation_token = response.get("NextContinuationToken")
        else:
            break

    return keys


def download(s3_url: str, filename: str, quiet: bool = False, client: Optional[BaseClient] = None) -> None:
    """Download the file at the S3 URL to the given local filename."""
    client = client or connect_r2()

    bucket, key = s3_bucket_key(s3_url)

    try:
        client.download_file(bucket, key, filename)  # type: ignore
    except ClientError as e:
        log.error(e)
        raise UploadError(e)

    if not quiet:
        log.info("DOWNLOADED", s3_url=s3_url, filename=filename)


def download_s3_folder(
    s3_folder: str,
    local_dir: Path,
    exclude: List[str] = [],
    include: List[str] = [],
    client: Optional[BaseClient] = None,
    max_workers: int = 20,
    delete: bool = False,
) -> None:
    """Download all files in the given S3 folder to the local directory."""
    assert s3_folder.endswith("/"), "s3_folder must end with a slash"

    client = client or connect_r2()

    bucket, _ = s3_bucket_key(s3_folder)

    if not local_dir.exists():
        local_dir.mkdir(parents=True)

    s3_keys = list_s3_objects(s3_folder, client=client)

    if exclude:
        s3_keys = [key for key in s3_keys if not any(pattern in key for pattern in exclude)]

    if include:
        s3_keys = [key for key in s3_keys if any(pattern in key for pattern in include)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for s3_key in s3_keys:
            local_file_path = local_dir / Path(s3_key).name
            futures.append(
                executor.submit(
                    download,
                    f"s3://{bucket}/{s3_key}",
                    local_file_path.as_posix(),
                    client=client,
                    quiet=True,
                )
            )

        concurrent.futures.wait(futures)

    if delete:
        local_files = set(local_dir.glob("*"))
        downloaded_files = set(local_dir / Path(s3_key).name for s3_key in s3_keys)
        files_to_delete = local_files - downloaded_files
        for file in files_to_delete:
            file.unlink()


def upload(s3_url: str, filename: str, public: bool = False, quiet: bool = False) -> None:
    """Upload the file at the given local filename to the S3 URL."""
    client = connect_r2()
    bucket, key = s3_bucket_key(s3_url)
    extra_args = {"ACL": "public-read"} if public else {}
    try:
        client.upload_file(filename, bucket, key, ExtraArgs=extra_args)
    except ClientError as e:
        log.error(e)
        raise UploadError(e)

    if not quiet:
        log.info(f"UPLOADED: {filename} -> {s3_url}")


# if R2_ACCESS_KEY and R2_SECRET_KEY are null, try using credentials from rclone config
def _read_owid_rclone_config() -> Dict[str, str]:
    # Create a ConfigParser object
    config = configparser.ConfigParser()

    # Read the configuration file
    config.read(os.path.expanduser("~/.config/rclone/rclone.conf"))

    return dict(config["owid-r2"].items())


def connect_r2() -> BaseClient:
    "Return a connection to Cloudflare's R2."
    import boto3

    # first, get the R2 credentials from dotenv
    R2_ACCESS_KEY = env.get("R2_ACCESS_KEY")
    R2_SECRET_KEY = env.get("R2_SECRET_KEY")
    R2_ENDPOINT = env.get("R2_ENDPOINT")
    R2_REGION_NAME = env.get("R2_REGION_NAME")

    # alternatively, get them from rclone config
    if not R2_ACCESS_KEY or not R2_SECRET_KEY or not R2_ENDPOINT:
        try:
            rclone_config = _read_owid_rclone_config()
            R2_ACCESS_KEY = R2_ACCESS_KEY or rclone_config.get("access_key_id")
            R2_SECRET_KEY = R2_SECRET_KEY or rclone_config.get("secret_access_key")
            R2_ENDPOINT = R2_ENDPOINT or rclone_config.get("endpoint")
            R2_REGION_NAME = R2_REGION_NAME or rclone_config.get("region")
        except KeyError:
            pass

    client = boto3.client(
        service_name="s3",
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        endpoint_url=R2_ENDPOINT or "https://078fcdfed9955087315dd86792e71a7e.r2.cloudflarestorage.com",
        region_name=R2_REGION_NAME or "auto",
    )

    return client


@lru_cache(maxsize=None)
def _connect_r2_cached() -> BaseClient:
    return connect_r2()


def connect_r2_cached() -> BaseClient:
    """Connect to R2, but cache the connection for subsequent calls. This is more efficient than
    creating a new connection for every request."""
    # creating a client is not thread safe, lock it
    with BOTO3_CLIENT_LOCK:
        return _connect_r2_cached()


class MissingCredentialsError(Exception):
    pass


class UploadError(Exception):
    pass

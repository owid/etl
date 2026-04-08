import concurrent.futures
import configparser
import os
import threading
from functools import lru_cache
from os import environ as env
from pathlib import Path
from urllib.parse import urlparse

# import botocore.client.S3 as BaseClient
import structlog
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError

log = structlog.get_logger()

BOTO3_CLIENT_LOCK = threading.Lock()


def s3_bucket_key(url: str) -> tuple[str, str]:
    """Extract bucket name and key from an S3 URL.

    Parses both `s3://` and `https://` S3 URLs to extract the bucket name
    and object key.

    Args:
        url: S3 URL in either format:
            - `s3://bucket-name/path/to/object`
            - `https://bucket-name.s3.region.amazonaws.com/path/to/object`

    Returns:
        Tuple of (bucket_name, object_key).

    Example:
        ```python
        # S3 protocol URL
        bucket, key = s3_bucket_key("s3://my-bucket/data/file.csv")
        # Returns: ("my-bucket", "data/file.csv")

        # HTTPS URL
        bucket, key = s3_bucket_key("https://my-bucket.s3.us-east-1.amazonaws.com/data/file.csv")
        # Returns: ("my-bucket", "data/file.csv")
        ```
    """
    parsed = urlparse(url)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    # strip region from bucket name in https scheme
    if parsed.scheme == "https":
        bucket = bucket.split(".")[0]

    return bucket, key


def list_s3_objects(s3_folder: str, client: BaseClient | None = None) -> list[str]:
    """List all objects in an S3 folder.

    Recursively lists all objects within an S3 folder, handling pagination
    automatically. Excludes folder markers (keys ending with '/').

    Args:
        s3_folder: S3 folder URL (e.g., `s3://bucket/path/to/folder/`).
        client: Optional boto3 S3 client. If None, connects to R2 automatically.

    Returns:
        List of object keys (paths) within the folder.

    Example:
        List all objects in a folder
        ```python
        objects = list_s3_objects("s3://my-bucket/data/")
        print(f"Found {len(objects)} objects")
        ```

        Use custom client
        ```python
        import boto3
        client = boto3.client('s3')
        objects = list_s3_objects("s3://my-bucket/data/", client=client)
        ```

    Note:
        This function handles pagination automatically for folders with
        more than 1000 objects.
    """
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


def download(s3_url: str, filename: str, quiet: bool = False, client: BaseClient | None = None) -> None:
    """Download a file from S3 to local filesystem.

    Args:
        s3_url: S3 URL of the file to download (e.g., `s3://bucket/path/file.csv`).
        filename: Local path where the file should be saved.
        quiet: If True, suppresses log messages. Defaults to False.
        client: Optional boto3 S3 client. If None, connects to R2 automatically.

    Raises:
        UploadError: If the download fails due to S3 client errors.

    Example:
        Download a file
        ```python
        download("s3://my-bucket/data/file.csv", "local_file.csv")
        ```

        Download quietly (no logs)
        ```python
        download("s3://my-bucket/data/file.csv", "file.csv", quiet=True)
        ```
    """
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
    exclude: list[str] = [],
    include: list[str] = [],
    client: BaseClient | None = None,
    max_workers: int = 20,
    delete: bool = False,
) -> None:
    """Download all files from an S3 folder to a local directory.

    Downloads all objects from an S3 folder using parallel threads for efficiency.
    Supports filtering with include/exclude patterns and optional deletion of
    local files not present in S3.

    Args:
        s3_folder: S3 folder URL. Must end with a slash (e.g., `s3://bucket/folder/`).
        local_dir: Local directory path where files will be downloaded.
        exclude: List of patterns to exclude from download. Files containing any
            of these patterns will be skipped.
        include: List of patterns to include in download. If specified, only files
            containing one of these patterns will be downloaded.
        client: Optional boto3 S3 client. If None, connects to R2 automatically.
        max_workers: Maximum number of parallel download threads. Defaults to 20.
        delete: If True, deletes local files that don't exist in the S3 folder.
            Defaults to False.

    Raises:
        AssertionError: If s3_folder doesn't end with a slash.
        UploadError: If any download fails.

    Example:
        Download entire folder
        ```python
        from pathlib import Path
        download_s3_folder(
            "s3://my-bucket/data/",
            Path("local_data")
        )
        ```

        Download only CSV files
        ```python
        download_s3_folder(
            "s3://my-bucket/data/",
            Path("local_data"),
            include=[".csv"]
        )
        ```

        Download and sync (delete local files not in S3)
        ```python
        download_s3_folder(
            "s3://my-bucket/data/",
            Path("local_data"),
            delete=True
        )
        ```

        Exclude backup files
        ```python
        download_s3_folder(
            "s3://my-bucket/data/",
            Path("local_data"),
            exclude=[".bak", ".tmp"]
        )
        ```

    Note:
        The local_dir is created automatically if it doesn't exist.
    """
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
        downloaded_files = {local_dir / Path(s3_key).name for s3_key in s3_keys}
        files_to_delete = local_files - downloaded_files
        for file in files_to_delete:
            file.unlink()


def upload(
    s3_url: str, filename: str | Path, public: bool = False, quiet: bool = False, downloadable: bool = False
) -> None:
    """Upload the file at the given local filename to the S3 URL.

    Args:
        s3_url: S3 URL to upload to
        filename: Local file to upload
        public: Whether to make the file publicly readable
        quiet: Whether to suppress log messages
        downloadable: If True, force browsers to download the file instead of displaying it inline. Sets Content-Disposition header to 'attachment; filename="..."'
    """
    client = connect_r2()
    bucket, key = s3_bucket_key(s3_url)
    extra_args = {"ACL": "public-read"} if public else {}

    # Add Content-Disposition header to force download with correct filename
    if downloadable:
        file_name = Path(filename).name
        extra_args["ContentDisposition"] = f'attachment; filename="{file_name}"'

    filename_str = str(filename)
    try:
        client.upload_file(filename_str, bucket, key, ExtraArgs=extra_args)
    except ClientError as e:
        log.error(e)
        raise UploadError(e)

    if not quiet:
        log.info(f"UPLOADED: {filename_str} -> {s3_url}")


# if R2_ACCESS_KEY and R2_SECRET_KEY are null, try using credentials from rclone config
def _read_owid_rclone_config() -> dict[str, str]:
    # Create a ConfigParser object
    config = configparser.ConfigParser()

    # Read the configuration file
    config.read(os.path.expanduser("~/.config/rclone/rclone.conf"))

    return dict(config["owid-r2"].items())


def connect_r2() -> BaseClient:
    """Create a connection to Cloudflare R2 storage.

    Creates a boto3 S3 client configured for Cloudflare R2. Credentials are loaded
    from environment variables or rclone configuration file.

    Credential sources (in priority order):

    1. Environment variables: `R2_ACCESS_KEY`, `R2_SECRET_KEY`, `R2_ENDPOINT`, `R2_REGION_NAME`
    2. rclone config file: `~/.config/rclone/rclone.conf` (section: `owid-r2`)

    Returns:
        Boto3 S3 client configured for R2.

    Example:
        ```python
        # Connect to R2
        client = connect_r2()

        # Use with boto3 operations
        client.list_objects_v2(Bucket='my-bucket', Prefix='data/')
        ```

    Note:
        For cached connections that reuse the same client across calls, use
        `connect_r2_cached()` instead. This is more efficient for multiple operations.

    See Also:
        - `connect_r2_cached()`: Thread-safe cached version
        - Cloudflare R2 docs: https://developers.cloudflare.com/r2/
    """
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

    cfg = Config(
        # These are necessary to avoid sending header `content-encoding: gzip,aws-chunked` which breaks Admin
        # see https://developers.cloudflare.com/r2/examples/aws/boto3/
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )

    client = boto3.client(
        service_name="s3",
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        endpoint_url=R2_ENDPOINT or "https://078fcdfed9955087315dd86792e71a7e.r2.cloudflarestorage.com",
        region_name=R2_REGION_NAME or "auto",
        config=cfg,
    )

    return client


@lru_cache(maxsize=None)
def _connect_r2_cached() -> BaseClient:
    return connect_r2()


def connect_r2_cached() -> BaseClient:
    """Create a cached, thread-safe connection to Cloudflare R2.

    Returns a cached R2 client that's reused across multiple calls. This is more
    efficient than creating a new connection for every request. Thread-safe through
    locking mechanism.

    Returns:
        Cached boto3 S3 client configured for R2.

    Example:
        Use cached connection for multiple operations
        ```python
        client = connect_r2_cached()
        client.upload_file('local.csv', 'bucket', 'remote.csv')
        client.download_file('bucket', 'data.json', 'local.json')
        # Both use the same underlying connection
        ```

    Note:
        The connection is cached indefinitely. If credentials change during runtime,
        the application needs to be restarted.

    See Also:
        - `connect_r2()`: Non-cached version for one-time connections
    """
    # creating a client is not thread safe, lock it
    with BOTO3_CLIENT_LOCK:
        return _connect_r2_cached()


class MissingCredentialsError(Exception):
    """Raised when R2 credentials are not found.

    This exception is raised when neither environment variables nor rclone
    configuration contain the required R2 credentials.
    """

    pass


class UploadError(Exception):
    """Raised when S3 upload or download operations fail.

    This exception wraps boto3 ClientError exceptions that occur during
    S3 operations like upload, download, or file listing.
    """

    pass

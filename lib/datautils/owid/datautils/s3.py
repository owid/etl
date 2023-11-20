"""S3 utils."""

import json
import os
import tempfile
from os import path
from typing import Any, List, Tuple, Union
from urllib.parse import urlparse

import pandas as pd
import structlog
from botocore.exceptions import ClientError

logger = structlog.get_logger()

S3_OBJECT = Union[dict, str, pd.DataFrame]

R2_ENDPOINT = os.environ.get("R2_ENDPOINT", "https://078fcdfed9955087315dd86792e71a7e.r2.cloudflarestorage.com")
AWS_PROFILE = os.environ.get("AWS_PROFILE", "default")


class S3:
    """S3 API class."""

    def __init__(self, profile_name: str = AWS_PROFILE) -> None:
        self.client = self.connect(profile_name)

    def s3_base(self, bucket: str) -> str:
        return f"s3://{bucket}"

    def http_base(self, bucket: str) -> str:
        if bucket.startswith("owid-"):
            return f"https://{bucket.replace('owid-', '')}.owid.io"
        else:
            raise NotImplementedError(f"Missing HTTP base for bucket {bucket}")

    def connect(self, profile_name: str = AWS_PROFILE) -> Any:
        """Return a connection to Walden's DigitalOcean space."""
        import boto3

        check_for_aws_profile(profile_name)

        session = boto3.Session(profile_name=profile_name)
        client = session.client(
            service_name="s3",
            endpoint_url=R2_ENDPOINT,
        )
        return client

    def list_files_in_folder(self, s3_path: str) -> List[str]:
        """List files in a folder within a bucket.

        Parameters
        ----------
        s3_path : str
             Path to S3 in format s3://mybucket/path/to/folder

        Returns
        -------
        list
             Objects found in folder.
        """
        if not s3_path.endswith("/"):
            s3_path += "/"

        bucket_name, s3_file = s3_path_to_bucket_key(s3_path)
        objects_request = self.client.list_objects_v2(Bucket=bucket_name, Prefix=s3_file)

        if objects_request["KeyCount"] == 0:
            return []

        if objects_request["KeyCount"] == objects_request["MaxKeys"]:
            logger.warning(
                "Too many objects to list. Consider using pagination.",
                bucket_name=bucket_name,
                s3_file=s3_file,
            )

        # List all objects with a prefix starting like the given path.
        objects_list = [obj["Key"] for obj in objects_request["Contents"]]

        return objects_list

    def upload_to_s3(
        self,
        local_path: str,
        s3_path: str,
        public: bool = False,
        quiet: bool = False,
    ) -> str:
        """
        Upload file to Walden.

        Parameters
        ----------
        local_path : str
            Local path to file.
        s3_path : str
            File location to load object from. e.g.
                s3://mybucket.nyc3.digitaloceanspaces.com/myfile.csv
                or
                s3://mybucket/myfile.csv
        public : bool
            Set to True to expose the file to the public (read only).

        Returns
        -------
        str
            URL of the file (`https://` if public, `s3://` if private)
        """
        if not quiet:
            logger.info("Uploading to S3…")
        # Obtain bucket & file
        bucket_name, s3_file = s3_path_to_bucket_key(s3_path)
        # Upload
        extra_args = {"ACL": "public-read"} if public else {}
        try:
            self.client.upload_file(local_path, bucket_name, s3_file, ExtraArgs=extra_args)
        except ClientError as e:
            logger.error(e)
            raise UploadError(e)

        if not quiet:
            logger.info("UPLOADED", s3_path=s3_path, local_path=local_path)

        if public:
            base = self.http_base(bucket_name)
        else:
            base = self.s3_base(bucket_name)
        return f"{base}/{s3_file}"

    def download_from_s3(
        self,
        s3_path: str,
        local_path: str,
        quiet: bool = False,
    ) -> None:
        """Download file from S3.

        Parameters
        ----------
        s3_path : str
            File location to load object from.
        local_path : str
            Path where to save file locally.
        """
        if not quiet:
            logger.info("Downloading from S3…")
        # Obtain bucket & file
        bucket_name, s3_file = s3_path_to_bucket_key(s3_path)
        # Download
        try:
            self.client.download_file(bucket_name, s3_file, local_path)
        except ClientError as e:
            logger.error(e)
            raise DownloadError(e)

        if not quiet:
            logger.info("DOWNLOADED", s3_path=s3_path, local_path=local_path)

        return

    def delete_from_s3(self, s3_path: str, quiet: bool = False) -> None:
        """Delete object at given S3 URL."""
        bucket_name, s3_file = s3_path_to_bucket_key(s3_path)

        try:
            self.client.delete_object(Bucket=bucket_name, Key=s3_file)
        except ClientError as e:
            logger.error(e)
            raise DeleteError(e)

        if not quiet:
            logger.info("DELETED", s3_path=s3_path)

    def obj_to_s3(self, obj: S3_OBJECT, s3_path: str, public: bool = False, **kwargs: Any) -> None:
        """Upload an object to S3, as a file.

        Parameters
        ----------
        obj: object
            Object to upload to S3. Currently:
                - dict -> JSON
                - str -> text
                - DataFrame -> CSV/XLSX/XLS/ZIP depending on `s3_path` value.
        s3_path : srt
            Object S3 file destination.
        public : bool, optional)
            Set to True if file is to be publicly accessed. Defaults to False.

        Raises
        ------
        ValueError
            If file format is not supported.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "file")
            if isinstance(obj, dict):
                with open(output_path, "w") as f:
                    json.dump(obj, f)
            elif isinstance(obj, str):
                with open(output_path, "w") as f:
                    f.write(obj)
            elif isinstance(obj, pd.DataFrame):
                if s3_path.endswith(".csv") or s3_path.endswith(".zip"):
                    obj.to_csv(output_path, index=False, **kwargs)
                elif s3_path.endswith(".xls") or s3_path.endswith(".xlsx"):
                    obj.to_excel(output_path, index=False, engine="xlsxwriter", **kwargs)
                else:
                    raise ValueError("pd.DataFrame must be exported to either CSV or XLS/XLSX!")
            else:
                raise ValueError(
                    f"Type of `obj` is not supported ({type(obj).__name__}). Supported"
                    " are json, str and pd.DataFrame"
                )
            self.upload_to_s3(local_path=output_path, s3_path=s3_path, public=public)

    def obj_from_s3(self, s3_path: str, **kwargs: Any) -> S3_OBJECT:
        """Load object from s3 location.

        Parameters
        ----------
        s3_path : str)
            File location to load object from.

        Returns
        -------
        object
            File loaded as object. Currently JSON -> dict, CSV/XLS/XLSV -> pd.DataFrame, general -> str
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "file")
            self.download_from_s3(s3_path=s3_path, local_path=output_path)
            if s3_path.endswith(".json"):
                with open(output_path, "r") as f:
                    return json.load(f)  # type: ignore
            elif s3_path.endswith(".csv"):
                return pd.read_csv(output_path, **kwargs)  # type: ignore
            elif s3_path.endswith(".xls") or s3_path.endswith(".xlsx"):
                return pd.read_excel(output_path, **kwargs)  # type: ignore
            else:
                with open(output_path, "r") as f:
                    return f.read()

    def get_metadata(self, s3_path: str) -> Any:
        """Get metadata from file `s3_path`.

        Parameters
        ----------
            s3_path (str): Path to S3 file.

        Returns
        -------
        dict
            Metadata
        """
        bucket_name, s3_file = s3_path_to_bucket_key(s3_path)
        response = self.client.head_object(Bucket=bucket_name, Key=s3_file)
        return response


def s3_path_to_bucket_key(url: str) -> Tuple[str, str]:
    """Get bucket and key from either s3:// URL or https:// URL."""
    parsed = urlparse(url)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    # strip region from bucket name in https scheme
    if parsed.scheme == "https":
        bucket = bucket.split(".")[0]

    return bucket, key


def check_for_aws_profile(profile_name: str) -> None:
    """Check that AWS config is correctly configured.

    You should have the credentials file at ~/.aws/config.
    """
    filename = path.expanduser("~/.aws/config")
    if not path.exists(filename) or f"[{profile_name}]" not in open(filename).read():
        raise FileExistsError(
            f"""you must set up a config file at ~/.aws/config
it should look like:

[{profile_name}]
aws_access_key_id = ...
aws_secret_access_key = ...
                """
        )

    return


def obj_to_s3(data: S3_OBJECT, s3_path: str, public: bool = False, **kwargs: Any) -> None:
    """See S3.obj_to_s3."""
    s3 = S3()
    return s3.obj_to_s3(data, s3_path, public, **kwargs)


def obj_from_s3(s3_path: str, **kwargs: Any) -> S3_OBJECT:
    """See S3.obj_from_s3."""
    s3 = S3()
    return s3.obj_from_s3(s3_path, **kwargs)


class UploadError(Exception):
    """Upload error."""

    pass


class DownloadError(Exception):
    """Download error."""

    pass


class DeleteError(Exception):
    """Delete error."""

    pass

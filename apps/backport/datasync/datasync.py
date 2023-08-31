import gzip
import json
import re
from typing import Any, Dict, Optional

import pandas as pd
import structlog
from botocore.exceptions import ClientError, EndpointConnectionError
from owid.catalog import s3_utils
from tenacity import Retrying
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from etl import config, files
from etl.publish import connect_s3_cached

log = structlog.get_logger()

config.enable_bugsnag()

import hashlib

import joblib


def upload_gzip_df(df: pd.DataFrame, s3_path: str, private: bool = False, r2: bool = False) -> None:
    """Upload compressed dataframe to S3 and return its URL."""

    bucket, key = s3_utils.s3_bucket_key(s3_path)

    client = connect_s3_cached(r2=r2)

    if r2:
        assert not private, "r2 does not support private files yet"
        extra_args = {}
    else:
        extra_args = {"ACL": "private" if private else "public-read"}

    # compare md5 of the file with the one in S3, if different, upload
    md5 = hashlib.sha256(pd.util.hash_pandas_object(df, index=False).values).hexdigest()
    # md5 = joblib.hash(df)

    try:
        response = client.head_object(Bucket=bucket, Key=key)
        s3_md5 = response["Metadata"].get("original-hash", None)
    except ClientError:
        s3_md5 = None

    if md5 != s3_md5:
        # TODO: use to_json directly
        d = df.to_dict(orient="list")
        s = json.dumps(d, default=str, sort_keys=True)
        body_gzip = gzip.compress(s.encode())  # type: ignore

        for attempt in Retrying(
            wait=wait_exponential(min=5, max=100),
            stop=stop_after_attempt(7),
            retry=retry_if_exception_type(EndpointConnectionError),
        ):
            with attempt:
                client.put_object(
                    Bucket=bucket,
                    Body=body_gzip,
                    Key=key,
                    ContentEncoding="gzip",
                    ContentType="application/json",
                    Metadata={"original-hash": md5},
                    **extra_args,
                )


def upload_gzip_dict(d: Dict[str, Any], s3_path: str, private: bool = False, r2: bool = False) -> None:
    """Upload compressed dictionary to S3 and return its URL."""
    bucket, key = s3_utils.s3_bucket_key(s3_path)

    client = connect_s3_cached(r2=r2)

    s = json.dumps(d, default=str, sort_keys=True)

    if r2:
        assert not private, "r2 does not support private files yet"
        extra_args = {}
    else:
        extra_args = {"ACL": "private" if private else "public-read"}

    # compare md5 of the file with the one in S3, if different, upload
    # NOTE: updatedAt is removed because it changes every time we upload
    # NOTE: uploading to S3 is actually not a bottleneck when using threads
    md5 = files.checksum_str("1" + re.sub(r'"updatedAt":\s*"[^"]*"', '"updatedAt": ""', s))

    try:
        response = client.head_object(Bucket=bucket, Key=key)
        s3_md5 = response["Metadata"].get("original-hash", None)
    except ClientError:
        s3_md5 = None

    if md5 != s3_md5:
        body_gzip = gzip.compress(s.encode())  # type: ignore

        for attempt in Retrying(
            wait=wait_exponential(min=5, max=100),
            stop=stop_after_attempt(7),
            retry=retry_if_exception_type(EndpointConnectionError),
        ):
            with attempt:
                client.put_object(
                    Bucket=bucket,
                    Body=body_gzip,
                    Key=key,
                    ContentEncoding="gzip",
                    ContentType="application/json",
                    Metadata={"original-hash": md5},
                    **extra_args,
                )

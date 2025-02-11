import gzip
import json
from typing import Any, Dict

import structlog
from botocore.exceptions import EndpointConnectionError, SSLError
from owid.catalog import s3_utils
from tenacity import Retrying
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from etl import config

log = structlog.get_logger()

config.enable_sentry()


def upload_gzip_dict(d: Dict[str, Any], s3_path: str, private: bool = False) -> None:
    return upload_gzip_string(json.dumps(d, default=str), s3_path=s3_path, private=private)


def upload_gzip_string(s: str, s3_path: str, private: bool = False) -> None:
    """Upload compressed dictionary to S3 and return its URL."""
    body_gzip = gzip.compress(s.encode())

    bucket, key = s3_utils.s3_bucket_key(s3_path)

    client = s3_utils.connect_r2_cached()

    assert not private, "r2 does not support private files yet"
    extra_args = {}

    for attempt in Retrying(
        wait=wait_exponential(min=5, max=100),
        stop=stop_after_attempt(7),
        retry=retry_if_exception_type((EndpointConnectionError, SSLError)),
    ):
        with attempt:
            client.put_object(  # type: ignore[reportAttributeAccessIssue]
                Bucket=bucket,
                Body=body_gzip,
                Key=key,
                ContentEncoding="gzip",
                ContentType="application/json",
                **extra_args,
            )

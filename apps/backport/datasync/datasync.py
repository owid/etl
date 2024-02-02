import gzip
import json
from typing import Any, Dict

import structlog
from botocore.exceptions import EndpointConnectionError
from owid.catalog import s3_utils
from tenacity import Retrying
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from etl import config
from etl.publish import connect_s3_cached

log = structlog.get_logger()

config.enable_bugsnag()


def upload_gzip_dict(d: Dict[str, Any], s3_path: str, private: bool = False) -> None:
    """Upload compressed dictionary to S3 and return its URL."""
    body_gzip = gzip.compress(json.dumps(d, default=str).encode())  # type: ignore

    bucket, key = s3_utils.s3_bucket_key(s3_path)

    client = connect_s3_cached()

    assert not private, "r2 does not support private files yet"
    extra_args = {}

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
                **extra_args,
            )

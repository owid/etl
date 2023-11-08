#
#  owid_cache.py
#
#  Helpers for working with our cache in Cloudflare R2.
#

from typing import Optional

from owid.datautils.s3 import S3

from .ui import log

S3_BASE = "s3://owid-walden"


def upload(filename: str, relative_path: str, public: bool = False) -> str:
    s3 = S3()
    return s3.upload_to_s3(filename, f"{S3_BASE}/{relative_path}", public)


def delete(relative_path: str, quiet: bool = False):
    s3 = S3()
    return s3.delete_from_s3(f"{S3_BASE}/{relative_path}")


def download(s3_url: str, filename: str, expected_md5: Optional[str] = None, quiet: bool = False) -> None:
    """Download the file at the S3 URL to the given local filename."""
    s3 = S3()
    if expected_md5:
        log("WARNING", "Expected MD5 check not implemented")
    return s3.download_from_s3(s3_url, filename)

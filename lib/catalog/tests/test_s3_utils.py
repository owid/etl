import pytest
from botocore.exceptions import ClientError

from owid.catalog.s3_utils import UploadError, object_exists, s3_bucket_key


class MockClient:
    """Minimal stand-in for a boto3 S3 client whose head_object raises a given ClientError."""

    def __init__(self, error: ClientError | None = None):
        self.error = error
        self.calls = []

    def head_object(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return {"ContentLength": 1}


def client_error(status: int) -> ClientError:
    return ClientError(
        {"Error": {"Code": str(status)}, "ResponseMetadata": {"HTTPStatusCode": status}},  # ty: ignore
        "HeadObject",
    )


def test_object_exists():
    client = MockClient()
    assert object_exists("s3://my-bucket/ab/cdef", client=client)  # ty: ignore
    assert client.calls == [{"Bucket": "my-bucket", "Key": "ab/cdef"}]


@pytest.mark.parametrize("status", [403, 404])
def test_object_exists_missing(status):
    """403 and 404 both mean 'not there' — 403 is what R2/S3 return when the caller can't list."""
    assert not object_exists("s3://my-bucket/ab/cdef", client=MockClient(client_error(status)))  # ty: ignore


def test_object_exists_raises_on_other_errors():
    """A failed check must not be reported as 'does not exist', or callers would re-upload blindly."""
    with pytest.raises(UploadError):
        object_exists("s3://my-bucket/ab/cdef", client=MockClient(client_error(500)))  # ty: ignore


def test_s3_bucket_key():
    assert s3_bucket_key("s3://my-bucket/data/file.csv") == ("my-bucket", "data/file.csv")
    assert s3_bucket_key("https://my-bucket.s3.us-east-1.amazonaws.com/data/file.csv") == (
        "my-bucket",
        "data/file.csv",
    )

# type: ignore
from unittest import mock

import boto3

from owid.datautils import s3


def test_s3_path_to_bucket_key():
    url = "https://walden.nyc3.digitaloceanspaces.com/a/test.csv"
    assert s3.s3_path_to_bucket_key(url) == ("walden", "a/test.csv")

    url = "s3://walden/a/test.csv"
    assert s3.s3_path_to_bucket_key(url) == ("walden", "a/test.csv")

    url = "https://walden.s3.us-west-2.amazonaws.com/a/test.csv"
    assert s3.s3_path_to_bucket_key(url) == ("walden", "a/test.csv")


@mock.patch.object(s3.S3, "connect")
def test_download_from_s3(connect_mock):
    S3 = s3.S3()
    S3.download_from_s3(
        "https://test_bucket.nyc3.digitaloceanspaces.com/test_bucket/test.csv",
        "test.csv",
    )
    assert connect_mock.return_value.download_file.call_args_list[0].args == (
        "test_bucket",
        "test_bucket/test.csv",
        "test.csv",
    )


@mock.patch.object(s3.S3, "connect")
def test_list_files_in_folder(connect_mock):
    url = "https://test_bucket.nyc3.digitaloceanspaces.com/test_bucket/"
    S3 = s3.S3()

    connect_mock.return_value.list_objects_v2.return_value = {
        "KeyCount": 0,
        "MaxKeys": "1000",
        "Contents": [{"Key": "test.csv"}, {"Key": "test_2.csv"}],
    }
    obj_list = S3.list_files_in_folder(url)
    assert obj_list == []

    connect_mock.return_value.list_objects_v2.return_value = {
        "KeyCount": 2,
        "MaxKeys": 2,
        "Contents": [{"Key": "test.csv"}, {"Key": "test_2.csv"}],
    }
    obj_list = S3.list_files_in_folder(url)
    assert obj_list == ["test.csv", "test_2.csv"]

    url = "https://test_bucket.nyc3.digitaloceanspaces.com/test_bucket"
    connect_mock.return_value.list_objects_v2.return_value = {
        "KeyCount": 2,
        "MaxKeys": 2,
        "Contents": [{"Key": "test.csv"}, {"Key": "test_2.csv"}],
    }
    obj_list = S3.list_files_in_folder(url)
    assert obj_list == ["test.csv", "test_2.csv"]


@mock.patch("owid.datautils.s3.check_for_aws_profile", return_value=None)
@mock.patch.object(boto3.Session, "__init__", return_value=None)
@mock.patch.object(boto3.Session, "client", return_value="client")
def test_connect(check_mocker, session_mocker_1, session_mocker_2):
    S3 = s3.S3()
    S3.connect()


@mock.patch.object(s3.S3, "connect")
def test_upload_to_s3(connect_mock):
    connect_mock.return_value.upload_file.return_value = None
    url = "s3://owid-walden/a/test.csv"
    S3 = s3.S3()
    s3_path = S3.upload_to_s3(s3_path=url, local_path="test.csv", public=True)
    assert s3_path == "https://walden.owid.io/a/test.csv"

    s3_path = S3.upload_to_s3(s3_path=url, local_path="test.csv", public=False)
    assert s3_path == "s3://owid-walden/a/test.csv"

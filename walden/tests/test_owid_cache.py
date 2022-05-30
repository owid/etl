from unittest import mock
from owid.walden.owid_cache import s3_bucket_key, download


def test_s3_bucket_key():
    url = 'https://walden.nyc3.digitaloceanspaces.com/a/test.csv'
    assert s3_bucket_key(url) == ('walden', 'a/test.csv')

    url = 's3://walden/a/test.csv'
    assert s3_bucket_key(url) == ('walden', 'a/test.csv')

    url = 'https://walden.s3.us-west-2.amazonaws.com/a/test.csv'
    assert s3_bucket_key(url) == ('walden', 'a/test.csv')


@mock.patch('owid.walden.owid_cache.connect')
def test_download(connect_mock):
    download(
        'https://test_bucket.nyc3.digitaloceanspaces.com/test_bucket/test.csv',
        'test.csv',
    )
    assert connect_mock.return_value.download_file.call_args_list[0].args == (
        'test_bucket',
        'test_bucket/test.csv',
        'test.csv'
    )

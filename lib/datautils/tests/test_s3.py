from owid.catalog import s3_utils


def test_s3_bucket_key():
    url = "https://api.owid.io/a/test.csv"
    assert s3_utils.s3_bucket_key(url) == ("api", "a/test.csv")

    url = "s3://api/a/test.csv"
    assert s3_utils.s3_bucket_key(url) == ("api", "a/test.csv")

    url = "https://api.s3.us-west-2.amazonaws.com/a/test.csv"
    assert s3_utils.s3_bucket_key(url) == ("api", "a/test.csv")

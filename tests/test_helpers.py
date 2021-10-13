#
#  test_helpers.py
#

from etl import helpers


# def test_get_github_sha():
#     sha = helpers.get_latest_github_sha("owid", "owid-grapher", "master")
#     assert re.match("^[0-9a-f]+$", sha)


def test_get_etag():
    etag = helpers.get_etag(
        "https://raw.githubusercontent.com/owid/owid-grapher/master/README.md"
    )
    assert etag

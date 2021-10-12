#
#  test_helpers.py
#

import re

from etl import helpers


def test_get_github_sha():
    sha = helpers.get_latest_github_sha("owid", "owid-grapher", "master")
    assert re.match("^[0-9a-f]+$", sha)

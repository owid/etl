#
#  test_helpers.py
#

import pytest

from etl import helpers


# def test_get_github_sha():
#     sha = helpers.get_latest_github_sha("owid", "owid-grapher", "master")
#     assert re.match("^[0-9a-f]+$", sha)

MY_SPECIAL_VAR: int = 1


def test_get_etag():
    etag = helpers.get_etag(
        "https://raw.githubusercontent.com/owid/owid-grapher/master/README.md"
    )
    assert etag


def test_run_isolated(tmp_path):
    # local process mutates the global
    assert _example_function() == 2
    assert _example_function() == 3

    # isolated process gets its own copy
    assert helpers.run_isolated(_example_function) == 2
    assert helpers.run_isolated(_example_function) == 2


def test_run_isolated_failure():
    with pytest.raises(Exception):
        helpers.run_isolated(_example_failure)


def _example_function() -> int:
    global MY_SPECIAL_VAR

    MY_SPECIAL_VAR += 1

    return MY_SPECIAL_VAR


def _example_failure() -> None:
    raise Exception("durian in coffee")

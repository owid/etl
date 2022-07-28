#
#  test_helpers.py
#

import pytest
import sys
from etl import helpers


# def test_get_github_sha():
#     sha = helpers.get_latest_github_sha("owid", "owid-grapher", "master")
#     assert re.match("^[0-9a-f]+$", sha)


def test_get_etag():
    etag = helpers.get_etag(
        "https://raw.githubusercontent.com/owid/owid-grapher/master/README.md"
    )
    assert etag


def test_isolated_env(tmp_path):
    (tmp_path / "shared.py").write_text("A = 1; import test_abc")
    (tmp_path / "test_abc.py").write_text("B = 1")

    with helpers.isolated_env(tmp_path):
        import shared

        assert shared.A == 1
        assert shared.test_abc.B == 1
        assert "test_abc" in sys.modules.keys()

    with pytest.raises(ModuleNotFoundError):
        import shared

    assert "test_abc" not in sys.modules.keys()

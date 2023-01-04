from etl import paths
from etl.helpers import PathFinder

#
#  test_helpers.py
#

# def test_get_github_sha():
#     sha = helpers.get_latest_github_sha("owid", "owid-grapher", "master")
#     assert re.match("^[0-9a-f]+$", sha)


def test_PathFinder_paths():
    def _assert(pf):
        assert pf.channel == "meadow"
        assert pf.namespace == "papers"
        assert pf.version == "2022-11-03"
        assert pf.short_name == "zijdeman_et_al_2015"

    # saved as short_name/__init__.py
    pf = PathFinder(str(paths.STEP_DIR / "data/meadow/papers/2022-11-03/zijdeman_et_al_2015/__init__.py"))
    _assert(pf)
    assert pf.directory == paths.STEP_DIR / "data/meadow/papers/2022-11-03/zijdeman_et_al_2015"

    # saved as short_name/anymodule.py
    pf = PathFinder(str(paths.STEP_DIR / "data/meadow/papers/2022-11-03/zijdeman_et_al_2015/anymodule.py"))
    _assert(pf)
    assert pf.directory == paths.STEP_DIR / "data/meadow/papers/2022-11-03/zijdeman_et_al_2015"

    # saved as short_name.py
    pf = PathFinder(str(paths.STEP_DIR / "data/meadow/papers/2022-11-03/zijdeman_et_al_2015.py"))
    _assert(pf)
    assert pf.directory == paths.STEP_DIR / "data/meadow/papers/2022-11-03"

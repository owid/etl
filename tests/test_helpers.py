import sys
from unittest.mock import patch

import pytest
from owid import catalog

from etl import paths
from etl.helpers import PathFinder, create_dataset, isolated_env


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


def test_create_dataset(tmp_path):
    meta = catalog.DatasetMeta(title="Test title")

    dest_dir = tmp_path / "data/garden/flowers/2020-01-01/rose"
    dest_dir.parent.mkdir(parents=True)

    # create metadata YAML file
    step_dir = tmp_path / "etl/steps"
    meta_yml = step_dir / "data/garden/flowers/2020-01-01/rose.meta.yml"
    meta_yml.parent.mkdir(parents=True)
    meta_yml.write_text(
        """
dataset:
    description: Test description
tables: {}""".strip()
    )

    # create dataset
    with patch("etl.paths.STEP_DIR", step_dir):
        ds = create_dataset(dest_dir, tables=[], default_metadata=meta)

    # check metadata
    assert ds.metadata.channel == "garden"
    assert ds.metadata.namespace == "flowers"
    assert ds.metadata.version == "2020-01-01"
    assert ds.metadata.short_name == "rose"
    assert ds.metadata.description == "Test description"
    assert ds.metadata.title == "Test title"


def test_PathFinder_with_private_steps():
    pf = PathFinder(str(paths.STEP_DIR / "data/garden/namespace/2023/name/__init__.py"))

    pf.dag = {
        "data://garden/namespace/2023/name": {
            "snapshot://namespace/2023/snapshot_a",
            "snapshot-private://namespace/2023/snapshot_b",
            # There could be two steps with the same name, one public and one private (odd case).
            "snapshot-private://namespace/2023/snapshot_a",
        }
    }
    assert pf.step == "data://garden/namespace/2023/name"
    assert pf.get_dependency_step_name("snapshot_a") == "snapshot://namespace/2023/snapshot_a"
    assert pf.get_dependency_step_name("snapshot_b") == "snapshot-private://namespace/2023/snapshot_b"
    # In the odd case that two dependencies have the same name, but one is public and the other is private,
    # assume it's public, unless explicitly stated otherwise.
    assert pf.get_dependency_step_name("snapshot_a", is_private=True) == "snapshot-private://namespace/2023/snapshot_a"

    pf.dag = {
        "data-private://garden/namespace/2023/name": {
            "snapshot-private://namespace/2023/name",
        }
    }
    assert pf.step == "data-private://garden/namespace/2023/name"
    assert pf.get_dependency_step_name("name") == "snapshot-private://namespace/2023/name"


def test_isolated_env(tmp_path):
    (tmp_path / "shared.py").write_text("A = 1; import test_abc")
    (tmp_path / "test_abc.py").write_text("B = 1")

    with isolated_env(tmp_path):
        import shared  # type: ignore

        assert shared.A == 1
        assert shared.test_abc.B == 1
        assert "test_abc" in sys.modules.keys()

    with pytest.raises(ModuleNotFoundError):
        import shared  # type: ignore

    assert "test_abc" not in sys.modules.keys()

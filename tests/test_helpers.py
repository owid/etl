import unittest
from unittest.mock import patch

from owid import catalog

import etl.helpers
from etl import paths
from etl.helpers import PathFinder, VersionTracker, create_dataset

# Dag of active steps.
mock_dag = {
    "steps": {
        "a": set(["b", "c"]),
        "b": set(["e", "d"]),
        "c": set(),
        "d": set(["e", "f"]),
        "e": set(),
        "f": set(),
    },
    "archive": {
        "g": set(["f"]),
        "h": set(["i", "j"]),
    },
}
# Expected set of all dependencies for each active step.
mock_expected_dependencies = {
    "a": set(["b", "c", "e", "d", "f"]),
    "b": set(["e", "d", "f"]),
    "c": set(),
    "d": set(["e", "f"]),
    "e": set(),
    "f": set(),
}
# Expected set of all usages for each active step.
mock_expected_usages = {
    "a": set(),
    "b": set(["a"]),
    "c": set(["a"]),
    "d": set(["b", "a"]),
    "e": set(["d", "b", "a"]),
    "f": set(["d", "b", "a", "g"]),
}
# Expected set of direct usages for each active step.
mock_expected_direct_usages = {
    "a": set(),
    "b": set(["a"]),
    "c": set(["a"]),
    "d": set(["b"]),
    "e": set(["d", "b"]),
    "f": set(["d", "g"]),
}
# For the previous mock dag to work, we have to structure the steps as data://channel/namespace/version/dataset.
# So I will assign an arbitrary prefix to all steps in the mock dag.
MOCK_STEP_PREFIX = "data://garden/institution_1/2023-01-27/dataset_"


def rename_steps_in_dag(dag, prefix):
    # This function takes a mock dag, and adapts its steps to have the expected ETL structure:
    # data://channel/namespace/version/dataset
    renamed_dag = {f"{prefix}{step}": set([f"{prefix}{substep}" for substep in dag[step]]) for step in dag}

    return renamed_dag


def create_mock_version_tracker(dag):
    def mock_load_dag(filename=paths.DEFAULT_DAG_FILE):
        # This function mimics load_dag, but using a custom dag.
        _dag = dag["steps"].copy()
        if filename == paths.DAG_ARCHIVE_FILE:
            _dag.update(dag["archive"])
        return rename_steps_in_dag(dag=_dag, prefix=MOCK_STEP_PREFIX)

    @patch.object(etl.helpers, "load_dag", mock_load_dag)
    def mock_version_tracker():
        # This function initializes VersionTracker using a mock dag.
        return VersionTracker()

    return mock_version_tracker()


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


def test_get_direct_dependencies_for_step_in_dag():
    for step in mock_dag["steps"]:
        dependencies = etl.helpers.get_direct_dependencies_for_step_in_dag(dag=mock_dag["steps"], step=step)
        expected_dependencies = mock_dag["steps"][step]
        assert sorted(dependencies) == sorted(expected_dependencies)


def test_get_direct_usages_for_step_in_dag():
    mock_dag_all = mock_dag["steps"].copy()
    mock_dag_all.update(mock_dag["archive"])
    for step in mock_dag["steps"]:
        usages = etl.helpers.get_direct_usages_for_step_in_dag(dag=mock_dag_all, step=step)
        assert sorted(usages) == sorted(mock_expected_direct_usages[step])


def test_get_all_dependencies_for_step_in_dag():
    for step in mock_dag["steps"]:
        dependencies = etl.helpers.get_all_dependencies_for_step_in_dag(dag=mock_dag["steps"], step=step)
        assert sorted(dependencies) == sorted(mock_expected_dependencies[step])


def test_get_all_usages_for_step_in_dag():
    mock_dag_all = mock_dag["steps"].copy()
    mock_dag_all.update(mock_dag["archive"])
    for step in mock_dag["steps"]:
        usages = etl.helpers.get_all_usages_for_step_in_dag(dag=mock_dag_all, step=step)
        assert sorted(usages) == sorted(mock_expected_usages[step])


def test_list_all_steps_in_dag():
    all_steps = etl.helpers.list_all_steps_in_dag(dag=mock_dag["steps"])
    assert sorted(all_steps) == sorted(mock_dag["steps"])


class TestVersionTracker(unittest.TestCase):
    def test_get_all_dependencies(self):
        versions = create_mock_version_tracker(dag=mock_dag)
        expected_dependencies = rename_steps_in_dag(dag=mock_expected_dependencies, prefix=MOCK_STEP_PREFIX)
        for step in rename_steps_in_dag(dag=mock_dag["steps"], prefix=MOCK_STEP_PREFIX):
            dependencies = versions.get_all_dependencies_for_step(step=step)
            assert sorted(dependencies) == sorted(expected_dependencies[step])

    def test_get_all_usages(self):
        versions = create_mock_version_tracker(dag=mock_dag)
        expected_usages = rename_steps_in_dag(dag=mock_expected_usages, prefix=MOCK_STEP_PREFIX)
        for step in rename_steps_in_dag(dag=mock_dag["steps"], prefix=MOCK_STEP_PREFIX):
            dependencies = versions.get_all_usages_for_step(step=step)
            assert sorted(dependencies) == sorted(expected_usages[step])

    def test_raise_error_if_latest_version_of_step_is_not_in_dag(self):
        # Remove a step from the dag that is one of the dependencies of another active step.
        _mock_dag = mock_dag.copy()
        del _mock_dag["steps"]["f"]
        versions = create_mock_version_tracker(dag=_mock_dag)
        with self.assertRaises(etl.helpers.LatestVersionOfStepShouldBeActive):
            versions.check_that_latest_version_of_steps_are_active()

    def test_raise_error_if_archived_step_should_be_active(self):
        # Include an archive step as a dependency of an active step.
        _mock_dag = mock_dag.copy()
        _mock_dag["steps"]["a"] = set(["b", "c", "g"])
        versions = create_mock_version_tracker(dag=_mock_dag)
        with self.assertRaises(etl.helpers.ArchiveStepUsedByActiveStep):
            versions.check_that_archive_steps_are_not_dependencies_of_active_steps()


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

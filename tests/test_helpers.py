import sys
from unittest.mock import patch

import pytest
from owid import catalog

import etl.helpers
from etl import paths
from etl.helpers import PathFinder, VersionTracker, create_dataset, isolated_env

# For the previous mock dag to work, we have to structure the steps as data://channel/namespace/version/dataset.
# So I will assign an arbitrary prefix to all steps in the mock dag.
MOCK_STEP_PREFIX = "data://garden/institution_1/2023-01-27/dataset_"


def rename_steps_in_dag(dag, prefix):
    # This function takes a mock dag, and adapts its steps to have the expected ETL structure:
    # data://channel/namespace/version/dataset
    renamed_dag = {f"{prefix}{step}": set([f"{prefix}{substep}" for substep in dag[step]]) for step in dag}

    return renamed_dag


def create_mock_version_tracker(dag, step_prefix=MOCK_STEP_PREFIX):
    def mock_load_dag(filename=paths.DEFAULT_DAG_FILE):
        # This function mimics load_dag, but using a custom dag.
        _dag = dag["steps"].copy()
        if filename == paths.DAG_ARCHIVE_FILE:
            _dag.update(dag["archive"])
        return rename_steps_in_dag(dag=_dag, prefix=step_prefix)

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


def test_get_direct_dependencies_for_step_in_dag(mock_dag):
    for step in mock_dag["steps"]:
        dependencies = etl.helpers.get_direct_dependencies_for_step_in_dag(dag=mock_dag["steps"], step=step)
        expected_dependencies = mock_dag["steps"][step]
        assert sorted(dependencies) == sorted(expected_dependencies)


def test_get_direct_usages_for_step_in_dag(mock_dag, mock_expected_direct_usages):
    mock_dag_all = mock_dag["steps"].copy()
    mock_dag_all.update(mock_dag["archive"])
    for step in mock_dag["steps"]:
        usages = etl.helpers.get_direct_usages_for_step_in_dag(dag=mock_dag_all, step=step)
        assert sorted(usages) == sorted(mock_expected_direct_usages[step])


def test_get_all_dependencies_for_step_in_dag(mock_dag, mock_expected_dependencies):
    for step in mock_dag["steps"]:
        dependencies = etl.helpers.get_all_dependencies_for_step_in_dag(dag=mock_dag["steps"], step=step)
        assert sorted(dependencies) == sorted(mock_expected_dependencies[step])


def test_get_all_usages_for_step_in_dag(mock_dag, mock_expected_usages):
    mock_dag_all = mock_dag["steps"].copy()
    mock_dag_all.update(mock_dag["archive"])
    for step in mock_dag["steps"]:
        usages = etl.helpers.get_all_usages_for_step_in_dag(dag=mock_dag_all, step=step)
        assert sorted(usages) == sorted(mock_expected_usages[step])


def test_list_all_steps_in_dag(mock_dag):
    all_steps = etl.helpers.list_all_steps_in_dag(dag=mock_dag["steps"])
    assert sorted(all_steps) == sorted(mock_dag["steps"])


def test_version_tracker_get_all_dependencies(mock_dag, mock_expected_dependencies):
    versions = create_mock_version_tracker(dag=mock_dag)
    expected_dependencies = rename_steps_in_dag(dag=mock_expected_dependencies, prefix=MOCK_STEP_PREFIX)
    for step in rename_steps_in_dag(dag=mock_dag["steps"], prefix=MOCK_STEP_PREFIX):
        dependencies = versions.get_all_dependencies_for_step(step=step)
        assert sorted(dependencies) == sorted(expected_dependencies[step])


def test_version_tracker_get_all_usages(mock_dag, mock_expected_usages):
    versions = create_mock_version_tracker(dag=mock_dag)
    expected_usages = rename_steps_in_dag(dag=mock_expected_usages, prefix=MOCK_STEP_PREFIX)
    for step in rename_steps_in_dag(dag=mock_dag["steps"], prefix=MOCK_STEP_PREFIX):
        dependencies = versions.get_all_usages_for_step(step=step)
        assert sorted(dependencies) == sorted(expected_usages[step])


def test_version_tracker_raise_error_if_latest_version_of_step_is_not_in_dag(mock_dag):
    # Remove a step from the dag that is one of the dependencies of another active step.
    _mock_dag = mock_dag.copy()
    del _mock_dag["steps"]["f"]
    versions = create_mock_version_tracker(dag=_mock_dag)
    with pytest.raises(etl.helpers.LatestVersionOfStepShouldBeActive):
        versions.check_that_latest_version_of_steps_are_active()


@patch("etl.helpers.log")
def test_version_tracker_raise_error_if_active_step_depends_on_archive_step(mock_log, mock_dag):
    # Include an archive step as a dependency of an active step.
    mock_dag["steps"]["a"] = set(["b", "c", "g"])
    versions = create_mock_version_tracker(dag=mock_dag)
    versions.check_that_active_dependencies_are_not_archived()
    mock_log.error.assert_called()
    # Checks for a specific substring of the logged message.
    assert "Missing" in mock_log.error.call_args[0][0]


@patch("etl.helpers.log")
def test_version_tracker_raise_error_if_active_step_depends_on_missing_step(mock_log, mock_dag):
    # Remove the definition of a step that is an active dependency of another step.
    del mock_dag["steps"]["b"]
    versions = create_mock_version_tracker(dag=mock_dag)
    versions.check_that_active_dependencies_are_defined()
    mock_log.error.assert_called()
    # Checks for a specific substring of the logged message.
    assert "Missing" in mock_log.error.call_args[0][0]


@patch("etl.helpers.log")
def test_version_tracker_raise_warning_if_active_steps_can_safely_be_archived(mock_log):
    mock_dag = {
        "steps": {
            # The following step is an old version of a that depends on old versions of b and c.
            "data://garden/institution_1/2024-01-01/dataset_a": set(
                ["data://garden/institution_1/2024-01-01/dataset_b", "data://garden/institution_1/2024-01-01/dataset_c"]
            ),
            # The following step is the latest version of a that depends on the latest version of b but an old version of c.
            "data://garden/institution_1/2024-01-02/dataset_a": set(
                ["data://garden/institution_1/2024-01-02/dataset_b", "data://garden/institution_1/2024-01-01/dataset_c"]
            ),
            # Old version of b.
            "data://garden/institution_1/2024-01-01/dataset_b": set(),
            # Latest version of b.
            "data://garden/institution_1/2024-01-02/dataset_b": set(),
            # Old version of c.
            "data://garden/institution_1/2024-01-01/dataset_c": set(),
            # Latest version of c.
            "data://garden/institution_1/2024-01-02/dataset_c": set(),
        },
        "archive": {},
    }
    versions = create_mock_version_tracker(dag=mock_dag, step_prefix="")
    versions.check_that_all_active_steps_are_necessary()
    mock_log.warning.assert_called()
    # Check that archivable steps appear in the warning.
    assert "data://garden/institution_1/2024-01-01/dataset_a" in mock_log.warning.call_args[0][0]
    assert "data://garden/institution_1/2024-01-01/dataset_b" in mock_log.warning.call_args[0][0]
    # Check that non-archivable steps do not appear in the warning.
    assert "data://garden/institution_1/2024-01-02/dataset_a" not in mock_log.warning.call_args[0][0]
    assert "data://garden/institution_1/2024-01-02/dataset_b" not in mock_log.warning.call_args[0][0]
    assert "data://garden/institution_1/2024-01-01/dataset_c" not in mock_log.warning.call_args[0][0]
    assert "data://garden/institution_1/2024-01-02/dataset_c" not in mock_log.warning.call_args[0][0]


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

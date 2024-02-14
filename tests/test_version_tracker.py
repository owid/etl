from unittest.mock import patch

import etl.version_tracker
from etl import paths
from etl.steps import reverse_graph
from etl.version_tracker import VersionTracker

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

    @patch.object(etl.version_tracker, "load_dag", mock_load_dag)
    def mock_version_tracker():
        # This function initializes VersionTracker using a mock dag.
        return VersionTracker(connect_to_db=False, warn_on_archivable=True)

    return mock_version_tracker()


def test_get_direct_dependencies_for_step_in_dag(mock_dag):
    for step in mock_dag["steps"]:
        dependencies = etl.version_tracker.get_direct_step_dependencies(dag=mock_dag["steps"], step=step)
        expected_dependencies = mock_dag["steps"][step]
        assert sorted(dependencies) == sorted(expected_dependencies)


def test_get_direct_usages_for_step_in_dag(mock_dag, mock_expected_direct_usages):
    mock_dag_all = mock_dag["steps"].copy()
    mock_dag_all.update(mock_dag["archive"])
    for step in mock_dag["steps"]:
        usages = etl.version_tracker.get_direct_step_usages(dag=mock_dag_all, step=step)
        assert sorted(usages) == sorted(mock_expected_direct_usages[step])


def test_get_all_dependencies_for_step_in_dag(mock_dag, mock_expected_dependencies):
    for step in mock_dag["steps"]:
        dependencies = etl.version_tracker.get_all_step_dependencies(dag=mock_dag["steps"], step=step)
        assert sorted(dependencies) == sorted(mock_expected_dependencies[step])


def test_get_all_usages_for_step_in_dag(mock_dag, mock_expected_usages):
    mock_dag_all = mock_dag["steps"].copy()
    mock_dag_all.update(mock_dag["archive"])
    mock_dag_all_reverse = reverse_graph(mock_dag_all)
    for step in mock_dag["steps"]:
        usages = etl.version_tracker.get_all_step_usages(dag_reverse=mock_dag_all_reverse, step=step)
        assert sorted(usages) == sorted(mock_expected_usages[step])


def test_list_all_steps_in_dag(mock_dag):
    all_steps = etl.version_tracker.list_all_steps_in_dag(dag=mock_dag["steps"])
    assert sorted(all_steps) == sorted(mock_dag["steps"])


def test_version_tracker_get_all_dependencies(mock_dag, mock_expected_dependencies):
    versions = create_mock_version_tracker(dag=mock_dag)
    expected_dependencies = rename_steps_in_dag(dag=mock_expected_dependencies, prefix=MOCK_STEP_PREFIX)
    for step in rename_steps_in_dag(dag=mock_dag["steps"], prefix=MOCK_STEP_PREFIX):
        dependencies = versions.get_all_step_dependencies(step=step)
        assert sorted(dependencies) == sorted(expected_dependencies[step])


def test_version_tracker_get_all_usages(mock_dag, mock_expected_usages):
    versions = create_mock_version_tracker(dag=mock_dag)
    expected_usages = rename_steps_in_dag(dag=mock_expected_usages, prefix=MOCK_STEP_PREFIX)
    for step in rename_steps_in_dag(dag=mock_dag["steps"], prefix=MOCK_STEP_PREFIX):
        dependencies = versions.get_all_step_usages(step=step)
        assert sorted(dependencies) == sorted(expected_usages[step])


@patch("etl.version_tracker.log")
def test_version_tracker_raise_error_if_active_step_depends_on_archive_step(mock_log, mock_dag):
    # Include an archive step as a dependency of an active step.
    mock_dag["steps"]["a"] = set(["b", "c", "g"])
    versions = create_mock_version_tracker(dag=mock_dag)
    versions.check_that_active_dependencies_are_not_archived()
    mock_log.error.assert_called()
    # Checks for a specific substring of the logged message.
    assert "Missing" in mock_log.error.call_args[0][0]


@patch("etl.version_tracker.log")
def test_version_tracker_raise_error_if_active_step_depends_on_missing_step(mock_log, mock_dag):
    # Remove the definition of a step that is an active dependency of another step.
    del mock_dag["steps"]["b"]
    versions = create_mock_version_tracker(dag=mock_dag)
    versions.check_that_active_dependencies_are_defined()
    mock_log.error.assert_called()
    # Checks for a specific substring of the logged message.
    assert "Missing" in mock_log.error.call_args[0][0]


@patch("etl.version_tracker.log")
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


def test_version_tracker_get_all_step_versions():
    mock_dag = {
        "steps": {
            "data://garden/institution_1/2024-01-01/dataset_a": set(
                ["data://garden/institution_1/2024-01-01/dataset_b", "data://garden/institution_1/2024-01-01/dataset_c"]
            ),
            "data://garden/institution_1/2024-01-02/dataset_a": set(
                ["data://garden/institution_1/2024-01-02/dataset_b", "data://garden/institution_1/2024-01-01/dataset_c"]
            ),
            "data://garden/institution_1/2024-01-03/dataset_a": set(),
            "data://garden/institution_1/2024-01-01/dataset_b": set(),
            "data://garden/institution_1/2024-01-02/dataset_c": set(),
        },
        "archive": {
            "data://garden/institution_1/2024-01-02/dataset_b": set(),
            "data://garden/institution_1/2024-01-01/dataset_c": set(),
        },
    }
    versions = create_mock_version_tracker(dag=mock_dag, step_prefix="")
    # Checks for dataset a.
    assert versions.get_backward_step_versions(step="data://garden/institution_1/2024-01-01/dataset_a") == []
    assert versions.get_backward_step_versions(step="data://garden/institution_1/2024-01-02/dataset_a") == [
        "data://garden/institution_1/2024-01-01/dataset_a"
    ]
    assert versions.get_backward_step_versions(step="data://garden/institution_1/2024-01-03/dataset_a") == [
        "data://garden/institution_1/2024-01-01/dataset_a",
        "data://garden/institution_1/2024-01-02/dataset_a",
    ]
    assert versions.get_forward_step_versions(step="data://garden/institution_1/2024-01-01/dataset_a") == [
        "data://garden/institution_1/2024-01-02/dataset_a",
        "data://garden/institution_1/2024-01-03/dataset_a",
    ]
    assert versions.get_forward_step_versions(step="data://garden/institution_1/2024-01-02/dataset_a") == [
        "data://garden/institution_1/2024-01-03/dataset_a",
    ]
    assert versions.get_forward_step_versions(step="data://garden/institution_1/2024-01-03/dataset_a") == []
    assert (
        versions.get_all_step_versions(step="data://garden/institution_1/2024-01-01/dataset_a")
        == versions.get_all_step_versions(step="data://garden/institution_1/2024-01-02/dataset_a")
        == versions.get_all_step_versions(step="data://garden/institution_1/2024-01-03/dataset_a")
        == [
            "data://garden/institution_1/2024-01-01/dataset_a",
            "data://garden/institution_1/2024-01-02/dataset_a",
            "data://garden/institution_1/2024-01-03/dataset_a",
        ]
    )
    # Checks for dataset b.
    assert versions.get_backward_step_versions(step="data://garden/institution_1/2024-01-01/dataset_b") == []
    assert versions.get_backward_step_versions(step="data://garden/institution_1/2024-01-02/dataset_b") == [
        "data://garden/institution_1/2024-01-01/dataset_b"
    ]
    assert versions.get_forward_step_versions(step="data://garden/institution_1/2024-01-01/dataset_b") == [
        "data://garden/institution_1/2024-01-02/dataset_b",
    ]
    assert versions.get_forward_step_versions(step="data://garden/institution_1/2024-01-02/dataset_b") == []
    assert (
        versions.get_all_step_versions(step="data://garden/institution_1/2024-01-01/dataset_b")
        == versions.get_all_step_versions(step="data://garden/institution_1/2024-01-02/dataset_b")
        == [
            "data://garden/institution_1/2024-01-01/dataset_b",
            "data://garden/institution_1/2024-01-02/dataset_b",
        ]
    )
    # Checks for dataset c.
    assert versions.get_backward_step_versions(step="data://garden/institution_1/2024-01-01/dataset_c") == []
    assert versions.get_backward_step_versions(step="data://garden/institution_1/2024-01-02/dataset_c") == [
        "data://garden/institution_1/2024-01-01/dataset_c"
    ]
    assert versions.get_forward_step_versions(step="data://garden/institution_1/2024-01-01/dataset_c") == [
        "data://garden/institution_1/2024-01-02/dataset_c",
    ]
    assert versions.get_forward_step_versions(step="data://garden/institution_1/2024-01-02/dataset_c") == []
    assert (
        versions.get_all_step_versions(step="data://garden/institution_1/2024-01-01/dataset_c")
        == versions.get_all_step_versions(step="data://garden/institution_1/2024-01-02/dataset_c")
        == [
            "data://garden/institution_1/2024-01-01/dataset_c",
            "data://garden/institution_1/2024-01-02/dataset_c",
        ]
    )

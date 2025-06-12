#
#  test_etl.py
#

"""
Check the integrity of the DAG.
"""

from pathlib import Path
from typing import List, Union
from unittest.mock import patch

from etl import paths
from etl.command import _detect_strictness_level, _grapher_steps
from etl.dag_helpers import load_dag
from etl.steps import DataStep, Step, compile_steps


def test_all_data_steps_have_code():
    # find all data steps
    steps = [s for s in get_all_steps() if isinstance(s, DataStep)]

    for s in steps:
        assert s.can_execute(), f'no code found for step "data://{s.path}"'


def test_sub_dag_import():
    # Ensure that sub-dag is imported from separate file
    assert any(["sub_dag_step" in step for step in load_dag("tests/data/dag.yml")]), "sub-dag steps not found"


@patch("etl.config.STRICT_AFTER", "2023-06-01")
def test_detect_strictness():
    for channel in ["meadow", "open_numbers"]:
        # lax channel with date after STRICT_AFTER
        step = DataStep(f"{channel}/_/2023-06-01/_", [])
        assert not _detect_strictness_level(step)

    channels = ["garden", "grapher"]
    for channel in channels:
        # strict channel with date before STRICT_AFTER
        step = DataStep(f"{channel}/_/2023-05-30/_", [])
        assert not _detect_strictness_level(step)

        # strict channel with date after STRICT_AFTER
        step = DataStep(f"{channel}/_/2023-06-01/_", [])
        assert _detect_strictness_level(step)

        # lax channel using "latest"
        # TODO: make this strict once the data passes
        step = DataStep(f"{channel}/_/latest/_", [])
        assert not _detect_strictness_level(step)


def get_all_steps(filename: Union[str, Path] = paths.DEFAULT_DAG_FILE) -> List[Step]:
    dag = load_dag(filename)

    # add grapher steps
    dag.update(_grapher_steps(dag, private=True))

    steps = compile_steps(dag, [])
    return steps


def test_get_exact_matches():
    dag = load_dag("tests/data/dag.yml")

    # Try all possible combinations of "exact_match" and "only" arguments, when passing the full step uri as arguments.
    assert [s.path for s in compile_steps(dag, includes=["data://test/step_1"], exact_match=True, only=True)] == [
        "test/step_1"
    ]
    assert [s.path for s in compile_steps(dag, includes=["data://test/step_1"], exact_match=True, only=False)] == [
        "test/step_0",
        "test/step_1",
    ]
    assert [s.path for s in compile_steps(dag, includes=["data://test/step_1"], exact_match=False, only=True)] == [
        "test/step_1"
    ]
    assert [s.path for s in compile_steps(dag, includes=["data://test/step_1"], exact_match=False, only=False)] == [
        "test/step_0",
        "test/step_1",
    ]

    # Try all possible combinations of "exact_match" and "only" arguments, when passing a substring of the step uri.
    assert [s.path for s in compile_steps(dag, includes=["step_1"], exact_match=True, only=True)] == []
    assert [s.path for s in compile_steps(dag, includes=["step_1"], exact_match=True, only=False)] == []
    assert [s.path for s in compile_steps(dag, includes=["step_1"], exact_match=False, only=True)] == ["test/step_1"]
    assert [s.path for s in compile_steps(dag, includes=["step_1"], exact_match=False, only=False)] == [
        "test/step_0",
        "test/step_1",
    ]

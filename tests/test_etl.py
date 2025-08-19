#
#  test_etl.py
#

"""
Check the integrity of the DAG.
"""

from pathlib import Path
from typing import List, Union

from etl import paths
from etl.command import _grapher_steps
from etl.dag_helpers import load_dag
from etl.steps import DataStep, Step, compile_steps, filter_to_subgraph


def test_all_data_steps_have_code():
    # find all data steps
    steps = [s for s in get_all_steps() if isinstance(s, DataStep)]

    for s in steps:
        assert s.can_execute(), f'no code found for step "data://{s.path}"'


def test_sub_dag_import():
    # Ensure that sub-dag is imported from separate file
    assert any(["sub_dag_step" in step for step in load_dag("tests/data/dag.yml")]), "sub-dag steps not found"




def get_all_steps(filename: Union[str, Path] = paths.DEFAULT_DAG_FILE) -> List[Step]:
    dag = load_dag(filename)

    # add grapher steps
    dag.update(_grapher_steps(dag, private=True))

    steps = compile_steps(dag, dag)
    return steps


def test_get_exact_matches():
    dag = load_dag("tests/data/dag.yml")

    # Try all possible combinations of "exact_match" and "only" arguments, when passing the full step uri as arguments.
    subdag = filter_to_subgraph(dag, includes=["data://test/step_1"], exact_match=True, only=True)
    assert [s.path for s in compile_steps(dag, subdag)] == ["test/step_1"]

    subdag = filter_to_subgraph(dag, includes=["data://test/step_1"], exact_match=True, only=False)
    assert [s.path for s in compile_steps(dag, subdag)] == ["test/step_0", "test/step_1"]

    subdag = filter_to_subgraph(dag, includes=["data://test/step_1"], exact_match=False, only=True)
    assert [s.path for s in compile_steps(dag, subdag)] == ["test/step_1"]

    subdag = filter_to_subgraph(dag, includes=["data://test/step_1"], exact_match=False, only=False)
    assert [s.path for s in compile_steps(dag, subdag)] == ["test/step_0", "test/step_1"]

    # Try all possible combinations of "exact_match" and "only" arguments, when passing a substring of the step uri.
    subdag = filter_to_subgraph(dag, includes=["step_1"], exact_match=True, only=True)
    assert [s.path for s in compile_steps(dag, subdag)] == []

    subdag = filter_to_subgraph(dag, includes=["step_1"], exact_match=True, only=False)
    assert [s.path for s in compile_steps(dag, subdag)] == []

    subdag = filter_to_subgraph(dag, includes=["step_1"], exact_match=False, only=True)
    assert [s.path for s in compile_steps(dag, subdag)] == ["test/step_1"]

    subdag = filter_to_subgraph(dag, includes=["step_1"], exact_match=False, only=False)
    assert [s.path for s in compile_steps(dag, subdag)] == ["test/step_0", "test/step_1"]

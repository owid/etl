#
#  test_etl.py
#

"""
Check the integrity of the DAG.
"""
from pathlib import Path
from typing import List, Union

from etl import paths
from etl.steps import DataStep, Step, WaldenStep, compile_steps, load_dag


def test_all_walden_deps_exist():
    # find all walden steps
    steps = [s for s in get_all_steps() if isinstance(s, WaldenStep)]

    # check that each step matches a dataset in walden's index
    for s in steps:
        assert s._walden_dataset, f'no walden data found for "walden://{s.path}"'


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
    steps = compile_steps(dag, [])
    return steps

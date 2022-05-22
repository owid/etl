#
#  test_etl.py
#

"""
Check the integrity of the DAG.
"""
from pathlib import Path
from typing import List, Union

from etl import paths
from etl.steps import load_dag, compile_steps, WaldenStep, DataStep, Step


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
    assert [
        s for s in get_all_steps(filename='tests/data/dag.yml') if "sub_dag" in s.path
    ], "sub-dag steps not found"


def get_all_steps(filename: Union[str, Path] = paths.DAG_FILE) -> List[Step]:
    dag = load_dag(filename)
    steps = compile_steps(dag, [])
    return steps

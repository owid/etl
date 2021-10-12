#
#  test_etl.py
#

"""
Check the integrity of the DAG.
"""

from typing import List

from etl import paths
from etl.steps import load_dag, parse_step, select_steps, WaldenStep, DataStep, Step


def test_all_walden_deps_exist():
    # find all walden steps
    steps = [s for s in get_all_steps() if isinstance(s, WaldenStep)]

    # check that each step matches a dataset in walden's index
    for s in steps:
        assert s._walden_dataset, f'no walden data found for "walden://{s.path}"'


def test_all_data_steps_have_code():
    # find all data steps
    steps = [
        s for s in get_all_steps() if isinstance(s, DataStep) and not s.is_reference()
    ]

    for s in steps:
        assert s.can_execute(), f'no code found for step "data://{s.path}"'


def get_all_steps() -> List[Step]:
    dag = load_dag(paths.DAG_FILE.as_posix())
    step_names = select_steps(dag, [])
    steps = [parse_step(name, dag) for name in step_names]
    return steps

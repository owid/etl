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
from etl.command import _detect_strictness_level
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

        # strict channel using "latest"
        step = DataStep(f"{channel}/_/latest/_", [])
        assert _detect_strictness_level(step)


def get_all_steps(filename: Union[str, Path] = paths.DEFAULT_DAG_FILE) -> List[Step]:
    dag = load_dag(filename)
    steps = compile_steps(dag, [])
    return steps

#
#  test_etl.py
#

from typing import List

from etl import command as cmd


def test_all_walden_deps_exist():
    # find all walden steps
    steps = [s for s in get_steps() if isinstance(s, cmd.WaldenStep)]

    # check that each step matches a dataset in walden's index
    for s in steps:
        assert s._walden_dataset, f'no walden data found for "walden://{s.path}"'


def test_all_data_steps_have_code():
    # find all data steps
    steps = [
        s for s in get_steps() if isinstance(s, cmd.DataStep) and not s.is_reference()
    ]

    for s in steps:
        assert s.can_execute(), f'no code found for step "data://{s.path}"'


def get_steps() -> List[cmd.Step]:
    dag = cmd.load_yaml(cmd.DAG_FILE.as_posix())
    step_names = cmd.select_steps(dag, [])
    steps = [cmd._parse_step(name, dag) for name in step_names]
    return steps

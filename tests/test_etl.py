#
#  test_etl.py
#

from etl import command as cmd


def test_all_walden_deps_exist():
    # find all walden steps
    dag = cmd.load_yaml(cmd.DAG_FILE.as_posix())
    step_names = cmd.select_steps(dag, [])
    steps = [cmd._parse_step(name, dag) for name in step_names]
    walden_steps = [s for s in steps if isinstance(s, cmd.WaldenStep)]

    # check that each step matches a dataset in walden's index
    for s in walden_steps:
        assert s._walden_dataset

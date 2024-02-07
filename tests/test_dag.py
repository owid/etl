#
#  test_dag.py
#
#  Test that the real DAG is intact and all its steps can run.
#


from etl import paths
from etl.command import construct_dag
from etl.steps import compile_steps


def test_steps_can_run():
    dag = construct_dag(paths.DEFAULT_DAG_FILE, backport=False, private=True, grapher=True)
    steps = compile_steps(dag, excludes=["backport"])
    for step in steps:
        if hasattr(step, "can_run"):
            assert step.can_run(archive_ok=False), f"{step} is missing resources and cannot run"  # type: ignore

#
#  test_command.py
#

"""
Test components of the etl command-line tool.
"""

import time

from etl import command as cmd


def test_timed_run():
    time_taken = cmd.timed_run(lambda: time.sleep(0.05))
    assert abs(time_taken - 0.05) < 0.01


def test_topological_sort():
    dag = {"steps": {"a": ["b", "c"], "b": ["c"]}}
    assert cmd.select_steps(dag, []) == ["c", "b", "a"]

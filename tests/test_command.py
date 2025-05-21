#
#  test_command.py
#

"""
Test components of the etl command-line tool.
"""

import time

import pytest

from etl import command as cmd
from etl.steps import compile_steps, DataStep, GrapherStep


def test_timed_run():
    time_taken = cmd.timed_run(lambda: time.sleep(0.05))
    assert abs(time_taken - 0.05) < 0.2


@pytest.fixture()
def dag():
    return {"data-private://a": {"data://b"}, "data://e": {"data://f"}}


def test_validate_private_steps(dag):
    cmd._validate_private_steps(compile_steps(dag))

    # public step with private dependency should raise an error
    new_dag = dict(
        dag,
        **{
            "data://c": {"data-private://d"},
        },
    )
    with pytest.raises(ValueError):
        cmd._validate_private_steps(compile_steps(new_dag))


def test_exec_graph_parallel():
    done = set()

    # Define a mock execution graph
    exec_graph = {
        "task1": [],
        "task2": ["task1"],
        "task3": ["task1"],
        "task4": ["task2", "task3"],
        "task5": ["task4"],
    }

    # Define a mock function for the task
    def mock_func(task: str, **kwargs):
        for dep in exec_graph[task]:
            assert dep in done
        done.add(task)

    # Execute the graph in parallel with 2 workers
    cmd.exec_graph_parallel(exec_graph, mock_func, workers=2, use_threads=True)

    # Assert that all tasks have been completed
    assert all(task in done for task in exec_graph.keys())


class DummyStep:
    """Minimal step used for dependency testing."""

    def __init__(self, name: str) -> None:
        self.path = name
        self.is_dirty = lambda: True


def test_set_dependencies_to_nondirty_data_step():
    dep1 = DummyStep("dep1")
    dep2 = DummyStep("dep2")
    step = DataStep("garden/ns/2020/test", [dep1, dep2])

    # both dependencies initially report dirty
    assert dep1.is_dirty() is True
    assert dep2.is_dirty() is True

    cmd._set_dependencies_to_nondirty(step)

    assert dep1.is_dirty() is False
    assert dep2.is_dirty() is False


def test_set_dependencies_to_nondirty_grapher_step():
    sub_dep = DummyStep("subdep")
    data_step = DataStep("garden/ns/2020/test", [sub_dep])
    data_step.is_dirty = lambda: True
    step = GrapherStep(data_step.path, [data_step])

    assert data_step.is_dirty() is True

    cmd._set_dependencies_to_nondirty(step)

    assert data_step.is_dirty() is False

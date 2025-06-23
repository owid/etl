#
#  test_command.py
#

"""
Test components of the etl command-line tool.
"""

import time

import pytest

from etl import command as cmd
from etl.steps import compile_steps


def test_timed_run():
    time_taken = cmd.timed_run(lambda: time.sleep(0.05))
    assert abs(time_taken - 0.05) < 0.2


@pytest.fixture()
def dag():
    return {"data-private://a": {"data://b"}, "data://e": {"data://f"}}


def test_validate_private_steps(dag):
    cmd._validate_private_steps(compile_steps(dag, dag))

    # public step with private dependency should raise an error
    new_dag = dict(
        dag,
        **{
            "data://c": {"data-private://d"},
        },
    )
    with pytest.raises(ValueError):
        cmd._validate_private_steps(compile_steps(new_dag, new_dag))


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


def test_construct_full_dag():
    """Test construct_full_dag function covers grapher step generation and export step handling."""
    # Test DAG with data://grapher step and export step
    dag = {
        "data://meadow/happiness/2023-01-01/happiness": {"snapshot://meadow/happiness/2023-01-01/happiness"},
        "data://garden/happiness/2023-01-01/happiness": {"data://meadow/happiness/2023-01-01/happiness"},
        "data://grapher/happiness/2023-01-01/happiness": {"data://garden/happiness/2023-01-01/happiness"},
        "export://explorers/happiness": {"data://grapher/happiness/2023-01-01/happiness"},
    }

    full_dag = cmd.construct_full_dag(dag)

    # Check that grapher step was generated
    expected_grapher_step = "grapher://grapher/happiness/2023-01-01/happiness"
    assert expected_grapher_step in full_dag
    assert full_dag[expected_grapher_step] == {"data://grapher/happiness/2023-01-01/happiness"}

    # Check that export step dependencies were updated to include grapher step
    assert expected_grapher_step in full_dag["export://explorers/happiness"]

    # Check that original dependencies are preserved
    assert "data://garden/happiness/2023-01-01/happiness" in full_dag
    assert "data://meadow/happiness/2023-01-01/happiness" in full_dag["data://garden/happiness/2023-01-01/happiness"]

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


def test_construct_subdag():
    """Test construct_subdag function filtering logic."""
    # Create a comprehensive DAG for testing
    full_dag = {
        "data://meadow/happiness/2023-01-01/happiness": {"snapshot://meadow/happiness/2023-01-01/happiness"},
        "data://garden/happiness/2023-01-01/happiness": {"data://meadow/happiness/2023-01-01/happiness"},
        "data://grapher/happiness/2023-01-01/happiness": {"data://garden/happiness/2023-01-01/happiness"},
        "grapher://grapher/happiness/2023-01-01/happiness": {"data://grapher/happiness/2023-01-01/happiness"},
        "export://explorers/happiness": {"data://grapher/happiness/2023-01-01/happiness"},
        "data-private://garden/secret/2023-01-01/secret": {"snapshot-private://meadow/secret/2023-01-01/secret"},
        "private://test/private_step": {"data-private://garden/secret/2023-01-01/secret"},
        "data://meadow/climate/2023-01-01/temperature": {"snapshot://meadow/climate/2023-01-01/temperature"},
    }

    # Test basic include filtering
    subdag = cmd.construct_subdag(full_dag, includes=["happiness"])
    happiness_steps = [step for step in subdag.keys() if "happiness" in step]
    assert len(happiness_steps) > 0
    assert "data://garden/happiness/2023-01-01/happiness" in subdag

    # Test exclude filtering
    subdag = cmd.construct_subdag(full_dag, includes=[".*"], excludes=["happiness"])
    happiness_steps = [step for step in subdag.keys() if "happiness" in step]
    assert len(happiness_steps) == 0

    # Test grapher step exclusion by default
    subdag = cmd.construct_subdag(full_dag, includes=["happiness"], grapher=False)
    grapher_steps = [step for step in subdag.keys() if step.startswith("grapher://")]
    assert len(grapher_steps) == 0

    # Test grapher step inclusion
    subdag = cmd.construct_subdag(full_dag, includes=["happiness"], grapher=True)
    grapher_steps = [step for step in subdag.keys() if step.startswith("grapher://")]
    assert len(grapher_steps) > 0

    # Test export step exclusion by default
    subdag = cmd.construct_subdag(full_dag, includes=["happiness"], export=False)
    export_steps = [step for step in subdag.keys() if step.startswith("export://")]
    assert len(export_steps) == 0

    # Test export step inclusion
    subdag = cmd.construct_subdag(full_dag, includes=["happiness"], export=True)
    export_steps = [step for step in subdag.keys() if step.startswith("export://")]
    assert len(export_steps) > 0

    # Test private step exclusion by default
    subdag = cmd.construct_subdag(full_dag, includes=[".*"], private=False)
    private_steps = [step for step in subdag.keys() if step.startswith("private://")]
    assert len(private_steps) == 0

    # Test private step inclusion
    subdag = cmd.construct_subdag(full_dag, includes=[".*"], private=True)
    private_steps = [step for step in subdag.keys() if step.startswith("private://")]
    assert len(private_steps) > 0

    # Test exact match - should include dependencies by default
    subdag = cmd.construct_subdag(full_dag, includes=["data://garden/happiness/2023-01-01/happiness"], exact_match=True)
    assert "data://garden/happiness/2023-01-01/happiness" in subdag
    # Dependencies are included by default unless only=True
    assert "data://meadow/happiness/2023-01-01/happiness" in subdag

    # Test exact match with only=True - should not include dependencies
    subdag = cmd.construct_subdag(
        full_dag, includes=["data://garden/happiness/2023-01-01/happiness"], exact_match=True, only=True
    )
    assert "data://garden/happiness/2023-01-01/happiness" in subdag
    assert "data://meadow/happiness/2023-01-01/happiness" not in subdag

    # Test only mode with filter_to_subgraph mock behavior
    # Note: This is a simplified test since only mode affects the filtering logic
    subdag = cmd.construct_subdag(full_dag, includes=["happiness"], only=True)
    # Should still include the step and its dependencies based on filter_to_subgraph behavior
    assert len(subdag) > 0


def test_construct_subdag_empty_includes():
    """Test construct_subdag with empty includes defaults to include all."""
    dag = {
        "data://garden/test/2023-01-01/test": {"data://meadow/test/2023-01-01/test"},
        "data://meadow/test/2023-01-01/test": {"snapshot://test/2023-01-01/test"},
    }

    subdag = cmd.construct_subdag(dag, includes=[])
    # Should include all non-excluded steps
    assert len(subdag) > 0


def test_construct_subdag_no_matches():
    """Test construct_subdag exits when no matches found."""
    dag = {
        "data://garden/test/2023-01-01/test": {"data://meadow/test/2023-01-01/test"},
    }

    # This should call sys.exit(1) since no steps match "nonexistent"
    with pytest.raises(SystemExit) as exc_info:
        cmd.construct_subdag(dag, includes=["nonexistent"])
    assert exc_info.value.code == 1

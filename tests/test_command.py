#
#  test_command.py
#

"""
Test components of the etl command-line tool.
"""

import os
import time

import pytest

from etl import command as cmd
from etl.steps import compile_steps


def test_timed_run():
    time_taken = cmd.timed_run(lambda: time.sleep(0.05))
    assert abs(time_taken - 0.05) < 0.2


@pytest.fixture(autouse=True)
def _clear_step_failures():
    # The failure list is module state, emptied by print_failure_recap() at the end of a run.
    cmd.STEP_FAILURES.clear()
    yield
    cmd.STEP_FAILURES.clear()


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
    cmd.exec_graph_parallel(exec_graph, mock_func, continue_on_failure=False, workers=2, use_threads=True)

    # Assert that all tasks have been completed
    assert all(task in done for task in exec_graph.keys())


def test_exec_graph_parallel_prints_traceback(capsys):
    def failing_func(task: str, **kwargs):
        raise ValueError(f"boom in {task}")

    # Without continue_on_failure the exception propagates, but only once the pool has drained
    # every already-submitted step, so the traceback has to be printed as soon as the step fails.
    with pytest.raises(ValueError):
        cmd.exec_graph_parallel({"task1": set()}, failing_func, continue_on_failure=False, workers=1, use_threads=True)

    captured = capsys.readouterr()
    assert "+++ Failed task1" in captured.out
    assert "ValueError: boom in task1" in captured.out


def test_exec_graph_parallel_reports_every_failure(capsys):
    def failing_func(task: str, **kwargs):
        raise ValueError(f"boom in {task}")

    # In continue_on_failure mode only the first exception is ever raised, so each failure needs
    # its own traceback in the log.
    with pytest.raises(ValueError):
        cmd.exec_graph_parallel(
            {"task1": set(), "task2": set()}, failing_func, continue_on_failure=True, workers=2, use_threads=True
        )

    captured = capsys.readouterr()
    assert "ValueError: boom in task1" in captured.out
    assert "ValueError: boom in task2" in captured.out


def test_failure_recap_repeats_every_traceback(capsys):
    from etl.steps import StepFailedError

    def failing_func(task: str, **kwargs):
        raise StepFailedError(f"TypeError: bad dtype in {task}\nStep {task} failed with exit code 1")

    with pytest.raises(StepFailedError):
        cmd.exec_graph_parallel(
            {"task1": set(), "task2": set()}, failing_func, continue_on_failure=True, workers=2, use_threads=True
        )
    # What the CLI does once the run is over, however it ended.
    cmd.print_failure_recap()

    captured = capsys.readouterr()
    # CI shows the tail of the log, where the re-raised exception contributes only executor
    # plumbing, so every failure's traceback is repeated in a recap at the end of the run.
    assert "+++ 2 step(s) failed" in captured.out
    for task in ("task1", "task2"):
        # Header and traceback have to be one write to one stream, or Buildkite interleaves them.
        assert captured.out.count(f"+++ Failed {task}\nTypeError: bad dtype in {task}") == 1
        assert captured.out.count(f">>> Failed {task}\nTypeError: bad dtype in {task}") == 1
    # Emptied, so a later run in the same process doesn't repeat these.
    assert cmd.STEP_FAILURES == []


def test_exec_graph_parallel_does_not_exit_silently_on_system_exit(capsys):
    # A step that raises SystemExit (as the forked step runner used to on failure) must not end the
    # run in silence: `except Exception` would not catch it and nothing would be printed.
    def exiting_func(task: str, **kwargs):
        raise SystemExit(1)

    with pytest.raises(SystemExit):
        cmd.exec_graph_parallel({"task1": set()}, exiting_func, continue_on_failure=False, workers=1, use_threads=True)

    captured = capsys.readouterr()
    assert "+++ Failed task1" in captured.out
    assert "SystemExit" in captured.out


def test_construct_full_dag():
    """Test construct_full_dag function covers grapher step generation and viz step handling."""
    # Test DAG with data://grapher step, and chart and explorer steps
    dag = {
        "data://meadow/happiness/2023-01-01/happiness": {"snapshot://meadow/happiness/2023-01-01/happiness"},
        "data://garden/happiness/2023-01-01/happiness": {"data://meadow/happiness/2023-01-01/happiness"},
        "data://grapher/happiness/2023-01-01/happiness": {"data://garden/happiness/2023-01-01/happiness"},
        "viz://explorer/happiness/latest/happiness": {"data://grapher/happiness/2023-01-01/happiness"},
        "viz://chart/happiness/latest/happiness": {"data://grapher/happiness/2023-01-01/happiness"},
        "viz://bespoke/happiness/latest/happiness": {"data://grapher/happiness/2023-01-01/happiness"},
        "viz://static/happiness/2023-01-01/happiness": {"data://grapher/happiness/2023-01-01/happiness"},
        # Export steps don't write to the DB, so they get no grapher:// dependency.
        "export://s3/happiness/latest/happiness": {"data://grapher/happiness/2023-01-01/happiness"},
    }

    full_dag = cmd.construct_full_dag(dag)

    # Check that grapher step was generated
    expected_grapher_step = "grapher://grapher/happiness/2023-01-01/happiness"
    assert expected_grapher_step in full_dag
    assert full_dag[expected_grapher_step] == {"data://grapher/happiness/2023-01-01/happiness"}

    # Check that the dependencies of steps writing to the grapher DB were updated to include the grapher step
    assert expected_grapher_step in full_dag["viz://explorer/happiness/latest/happiness"]
    assert expected_grapher_step in full_dag["viz://chart/happiness/latest/happiness"]
    assert expected_grapher_step in full_dag["viz://bespoke/happiness/latest/happiness"]
    assert expected_grapher_step in full_dag["viz://static/happiness/2023-01-01/happiness"]
    assert expected_grapher_step not in full_dag["export://s3/happiness/latest/happiness"]

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
        "viz://explorer/happiness/latest/happiness": {
            "data://grapher/happiness/2023-01-01/happiness",
            "grapher://grapher/happiness/2023-01-01/happiness",
        },
        "viz://chart/happiness/latest/happiness": {
            "data://grapher/happiness/2023-01-01/happiness",
            "grapher://grapher/happiness/2023-01-01/happiness",
        },
        "viz://static/happiness/2023-01-01/happiness": {"data://garden/happiness/2023-01-01/happiness"},
        "viz://bespoke/happiness/latest/happiness": {"data://garden/happiness/2023-01-01/happiness"},
        "export://s3/happiness/latest/happiness": {"data://garden/happiness/2023-01-01/happiness"},
        "data-private://garden/secret/2023-01-01/secret": {"snapshot-private://meadow/secret/2023-01-01/secret"},
        "data-private://grapher/secret/2023-01-01/secret": {"data-private://garden/secret/2023-01-01/secret"},
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

    # With no flag, only data steps run: no grapher:// upserts, no viz:// steps, no export:// steps.
    subdag = cmd.construct_subdag(full_dag, includes=["happiness"])
    assert "data://garden/happiness/2023-01-01/happiness" in subdag
    assert not [step for step in subdag if step.startswith(("grapher://", "viz://", "export://"))]

    # --grapher runs the steps writing to the grapher DB: grapher:// upserts and every viz:// step
    subdag = cmd.construct_subdag(full_dag, includes=["happiness"], grapher=True)
    for channel in ["chart", "explorer", "bespoke"]:
        assert f"viz://{channel}/happiness/latest/happiness" in subdag
    assert "viz://static/happiness/2023-01-01/happiness" in subdag
    assert "grapher://grapher/happiness/2023-01-01/happiness" in subdag
    assert not [step for step in subdag if step.startswith("export://")]

    # --export runs the export:// steps, which write to external destinations.
    # It no longer implies --grapher: grapher:// upserts and viz steps stay excluded.
    subdag = cmd.construct_subdag(full_dag, includes=["happiness"], export=True)
    assert "export://s3/happiness/latest/happiness" in subdag
    assert not [step for step in subdag if step.startswith(("grapher://", "viz://"))]

    # Private steps run by default; --public-only (private=False) skips them
    subdag = cmd.construct_subdag(full_dag, includes=[".*"])
    private_steps = [step for step in subdag.keys() if "-private://" in step]
    assert len(private_steps) > 0

    subdag = cmd.construct_subdag(full_dag, includes=[".*"], private=False)
    private_steps = [step for step in subdag.keys() if "-private://" in step]
    assert len(private_steps) == 0

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


GATED_DAG = {
    "data://garden/happiness/2023-01-01/happiness": set(),
    "data://grapher/happiness/2023-01-01/happiness": {"data://garden/happiness/2023-01-01/happiness"},
    "grapher://grapher/happiness/2023-01-01/happiness": {"data://grapher/happiness/2023-01-01/happiness"},
    "viz://chart/happiness/latest/happiness": {
        "data://grapher/happiness/2023-01-01/happiness",
        "grapher://grapher/happiness/2023-01-01/happiness",
    },
    "viz://chart/happiness/latest/happiness_extended": {"data://grapher/happiness/2023-01-01/happiness"},
    "export://s3/happiness/latest/happiness": {"data://grapher/happiness/2023-01-01/happiness"},
}


def test_construct_subdag_named_gated_steps_are_selected_without_their_flag(capsys):
    """Naming a gated step type selects it; the flag only decides whether it may write."""
    # A viz step named without --grapher is selected with its data dependencies; the grapher://
    # upsert of its input is left out, with a note.
    subdag = cmd.construct_subdag(GATED_DAG, includes=["viz://chart/happiness/latest/happiness"])
    assert "viz://chart/happiness/latest/happiness" in subdag
    assert "data://grapher/happiness/2023-01-01/happiness" in subdag
    assert not [step for step in subdag if step.startswith("grapher://")]
    assert "grapher://grapher/happiness/2023-01-01/happiness" not in subdag["viz://chart/happiness/latest/happiness"]
    assert "Skipping 1 grapher:// upsert(s) without --grapher" in capsys.readouterr().out

    # With the flag, the upsert comes along.
    subdag = cmd.construct_subdag(GATED_DAG, includes=["viz://chart/happiness/latest/happiness"], grapher=True)
    assert "grapher://grapher/happiness/2023-01-01/happiness" in subdag

    # A scheme prefix counts as naming the type too.
    subdag = cmd.construct_subdag(GATED_DAG, includes=["viz://chart"])
    assert {s for s in subdag if s.startswith("viz://")} == {
        "viz://chart/happiness/latest/happiness",
        "viz://chart/happiness/latest/happiness_extended",
    }

    # Same for export:// without --export.
    subdag = cmd.construct_subdag(GATED_DAG, includes=["export://s3/happiness/latest/happiness"])
    assert "export://s3/happiness/latest/happiness" in subdag

    # A named grapher:// step without --grapher: its data dependencies are selected, the upsert is not.
    subdag = cmd.construct_subdag(GATED_DAG, includes=["grapher://grapher/happiness/2023-01-01/happiness"])
    assert "data://grapher/happiness/2023-01-01/happiness" in subdag
    assert not [step for step in subdag if step.startswith("grapher://")]


def test_construct_subdag_patterns_still_skip_gated_steps_without_their_flag(capsys):
    """A plain pattern never pulls in a gated step type without its flag: `etlr '.*'` stays safe."""
    subdag = cmd.construct_subdag(GATED_DAG, includes=["happiness"])
    assert not [step for step in subdag if step.startswith(("grapher://", "viz://", "export://"))]
    assert "Skipping" not in capsys.readouterr().out

    # A named step and a pattern can be combined; the pattern's gate doesn't leak onto the named step.
    subdag = cmd.construct_subdag(GATED_DAG, includes=["garden", "export://s3/happiness/latest/happiness"])
    assert "export://s3/happiness/latest/happiness" in subdag
    assert not [step for step in subdag if step.startswith(("grapher://", "viz://"))]


def test_main_sets_write_permissions_for_programmatic_callers(monkeypatch):
    """`etl browser` and fasttrack call `main()` directly, so the write switches must be set there, not
    only in `main_cli`: a named viz step run from the browser without its permission must not publish."""
    from etl import config

    dag = {"data://garden/happiness/2023-01-01/happiness": {"snapshot://meadow/happiness/2023-01-01/happiness"}}
    monkeypatch.setattr(cmd, "load_dag", lambda dag_path: dag)
    monkeypatch.setattr(cmd, "run_steps", lambda *args, **kwargs: None)
    monkeypatch.setattr(cmd, "sanity_check_db_settings", lambda grapher_user_id: None)
    monkeypatch.setattr(config, "GRAPHER_ENABLED", True)
    monkeypatch.setattr(config, "EXPORT_ENABLED", True)
    monkeypatch.setenv("GRAPHER_ENABLED", "1")
    monkeypatch.setenv("EXPORT_ENABLED", "1")

    cmd.main(includes=["happiness"])
    assert (config.GRAPHER_ENABLED, config.EXPORT_ENABLED) == (False, False)
    assert (os.environ["GRAPHER_ENABLED"], os.environ["EXPORT_ENABLED"]) == ("0", "0")

    cmd.main(includes=["happiness"], grapher=True)
    assert (config.GRAPHER_ENABLED, config.EXPORT_ENABLED) == (True, False)
    assert os.environ["GRAPHER_ENABLED"] == "1"


def test_construct_subdag_public_only_warns_about_dropped_named_steps(capsys):
    """A pattern silently skips the public steps downstream of a private one; a named step says so."""
    dag = {
        "data-private://garden/secret/2023-01-01/secret": {"snapshot-private://meadow/secret/2023-01-01/secret"},
        "data://garden/mixed/2023-01-01/mixed": {"data-private://garden/secret/2023-01-01/secret"},
        "data://garden/open/2023-01-01/open": {"snapshot://meadow/open/2023-01-01/open"},
    }

    subdag = cmd.construct_subdag(dag, includes=["garden"], private=False)
    assert set(subdag) == {"data://garden/open/2023-01-01/open", "snapshot://meadow/open/2023-01-01/open"}
    assert "Skipping" not in capsys.readouterr().out

    subdag = cmd.construct_subdag(
        dag, includes=["data://garden/mixed/2023-01-01/mixed", "data://garden/open/2023-01-01/open"], private=False
    )
    assert "data://garden/mixed/2023-01-01/mixed" not in subdag
    assert "data://garden/open/2023-01-01/open" in subdag
    out = capsys.readouterr().out
    assert "Skipping 1 named step(s) with --public-only" in out
    assert "data://garden/mixed/2023-01-01/mixed" in out


def test_construct_subdag_full_step_name_selects_that_step_only():
    """`viz://chart/.../happiness` must not also select `.../happiness_extended`."""
    subdag = cmd.construct_subdag(GATED_DAG, includes=["viz://chart/happiness/latest/happiness"], grapher=True)
    assert "viz://chart/happiness/latest/happiness_extended" not in subdag

    # A prefix is still a pattern.
    subdag = cmd.construct_subdag(GATED_DAG, includes=["viz://chart/happiness/latest/happi"], grapher=True)
    assert "viz://chart/happiness/latest/happiness_extended" in subdag


def test_modified_steps_matches_full_uri_includes(monkeypatch):
    """Changed data steps come back scheme-less, so `etlr data://garden/... --modified` must still match them."""
    import etl.io

    changed = [
        "garden/happiness/2023-01-01/happiness",
        "garden/happiness/2023-01-01/happiness_extended",
        "explorers/happiness/latest/happiness",
        "viz://chart/happiness/latest/happiness",
    ]
    monkeypatch.setattr(etl.io, "get_all_changed_catalog_paths", lambda files_changed, include_export: changed)

    # A full URI selects that step only, not `happiness_extended` as well.
    assert cmd._modified_steps(includes=["data://garden/happiness/2023-01-01/happiness"], files_changed={}) == [
        "garden/happiness/2023-01-01/happiness"
    ]
    # A prefix is still a pattern.
    assert cmd._modified_steps(includes=["data://garden/happiness/2023-01-01/happi"], files_changed={}) == [
        "garden/happiness/2023-01-01/happiness",
        "garden/happiness/2023-01-01/happiness_extended",
    ]
    assert cmd._modified_steps(includes=["viz://chart/happiness"], files_changed={}) == [
        "viz://chart/happiness/latest/happiness"
    ]
    # A viz:// include keeps its scheme: `viz://explorer` must not turn into the bare `explorer` and match
    # the `explorers/...` data step (ops selects the mdim pass with `viz://chart viz://explorer --modified`).
    assert cmd._modified_steps(includes=["viz://explorer"], files_changed={}) == []
    # Plain patterns keep working, and match every kind.
    assert cmd._modified_steps(includes=["happiness"], files_changed={}) == changed

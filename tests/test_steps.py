#
#  test_steps.py
#

"""
Test that the different step types work as expected.
"""

from contextlib import contextmanager
from typing import Iterator
import random
import string
import shutil
from unittest.mock import patch

from owid.catalog import Dataset

from etl import paths
from etl.steps import (
    DataStep,
    DataStepPrivate,
    BackportStepPrivate,
    compile_steps,
    to_dependency_order,
    select_dirty_steps,
    Step,
    filter_to_subgraph,
)


def _create_mock_py_file(step_name: str) -> None:
    py_file = paths.STEP_DIR / "data" / f"{step_name}.py"
    assert not py_file.exists()
    with open(str(py_file), "w") as ostream:
        print(
            """
from owid.catalog import Dataset
def run(dest_dir):
    Dataset.create_empty(dest_dir)
            """,
            file=ostream,
        )


def test_data_step():
    with temporary_step() as step_name:
        _create_mock_py_file(step_name)
        DataStep(step_name, []).run()
        Dataset((paths.DATA_DIR / step_name).as_posix())


@contextmanager
def temporary_step() -> Iterator[str]:
    "Make a step in the etl/ directory, but clean up afterwards."
    name = "".join(random.choice(string.ascii_lowercase) for i in range(10))
    try:
        yield name
    finally:
        data_dir = paths.DATA_DIR / name

        if data_dir.is_dir():
            shutil.rmtree(data_dir.as_posix())

        py_file = paths.STEP_DIR / "data" / f"{name}.py"
        ipy_file = paths.STEP_DIR / "data" / f"{name}.ipynb"

        if py_file.exists():
            py_file.unlink()

        if ipy_file.exists():
            ipy_file.unlink()


def test_dependency_ordering():
    "Check that a dependency will be scheduled to run before things that need it."
    dag = {"a": ["b", "c"], "b": ["c"]}
    assert to_dependency_order(dag, [], []) == ["c", "b", "a"]


def test_dependency_filtering():
    dag = {
        "e": {"a"},
        "c": {"b", "d"},
        "b": {"a"},
    }
    assert filter_to_subgraph(dag, ["b"]) == {
        "d": set(),
        "c": {"b", "d"},
        "b": {"a"},
        "a": set(),
    }
    assert filter_to_subgraph(dag, ["b"], incl_forward=False) == {
        "b": {"a"},
        "a": set(),
    }


@patch("etl.steps.parse_step")
def test_selection_selects_children(parse_step):
    """When you pick a step, it should rebuild everything that depends on that step
    and all dependencies of those steps"""
    parse_step.side_effect = lambda name, _: DummyStep(name)

    dag = {"a": ["b", "c"], "d": ["a"], "e": ["b"]}

    # selecting "c" should cause "c" -> "a" -> "d" to all be selected
    #                            "b" to be ignored
    steps = compile_steps(dag, ["c"], [])
    assert len(steps) == 4
    assert set(s.path for s in steps) == {"b", "c", "a", "d"}


@patch("etl.steps.parse_step")
def test_selection_selects_parents(parse_step):
    "When you pick a step, it should select everything that step depends on."
    parse_step.side_effect = lambda name, _: DummyStep(name)

    dag = {"a": ["b"], "d": ["a"], "c": ["a"]}

    # selecting "d" should cause "b" -> "a" -> "d" to all be selected
    #                            "c" to be ignored
    steps = compile_steps(dag, ["d"], [])
    assert len(steps) == 3
    assert set(s.path for s in steps) == {"b", "a", "d"}


class DummyStep(Step):
    def __init__(self, name: str):
        self.path = name

    def __repr__(self):
        return self.path


def test_data_step_private():
    with temporary_step() as step_name:
        _create_mock_py_file(step_name)
        DataStepPrivate(step_name, []).run()
        ds = Dataset((paths.DATA_DIR / step_name).as_posix())
        assert not ds.metadata.is_public


@patch("etl.backport_helpers.create_dataset")
def test_backport_step_private(mock_create_dataset):
    with temporary_step() as step_name:
        path = paths.DATA_DIR / step_name
        mock_create_dataset.return_value = Dataset.create_empty(path)
        BackportStepPrivate(step_name, []).run()
        assert not Dataset(path).metadata.is_public


def test_select_dirty_steps():
    """select_dirty_steps should only select dirty steps, this can be tricky when using threads"""
    steps = [DummyStep(f"{i}") for i in range(20)]
    for s in steps:
        if random.random() < 0.5:
            s.is_dirty = lambda: False  # type: ignore
        else:
            s.is_dirty = lambda: True  # type: ignore
    assert all([s.is_dirty() for s in select_dirty_steps(steps, 10)])  # type: ignore

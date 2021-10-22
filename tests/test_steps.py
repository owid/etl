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

from owid.catalog import Dataset

from etl import paths
from etl.steps import DataStep, to_dependency_order


def test_data_step():
    with temporary_step() as step_name:
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


def test_topological_sort():
    "Check that a dependency will be scheduled to run before things that need it."
    dag = {"a": ["b", "c"], "b": ["c"]}
    assert to_dependency_order(dag, [], []) == ["c", "b", "a"]

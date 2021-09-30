#
#  test_command.py
#

from pathlib import Path
import time
from contextlib import contextmanager
import random
import string
import shutil
from typing import Iterator

from etl import command as cmd
from owid.catalog import Dataset


def test_timed_run():
    time_taken = cmd.timed_run(lambda: time.sleep(0.05))
    assert abs(time_taken - 0.05) < 0.01


def test_topological_sort():
    dag = {"steps": {"a": ["b", "c"], "b": ["c"]}}
    assert cmd.select_steps(dag, []) == ["c", "b", "a"]


def test_data_step():
    with temporary_step() as step_name:
        py_file = Path(cmd.STEP_DIR) / "data" / f"{step_name}.py"
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

        cmd.DataStep(step_name, []).run()

        Dataset((Path(cmd.DATA_DIR) / step_name).as_posix())


@contextmanager
def temporary_step() -> Iterator[str]:
    "Make a step in the etl/ directory, but clean up afterwards."
    name = "".join(random.choice(string.ascii_lowercase) for i in range(10))
    try:
        yield name
    finally:
        data_dir = Path(cmd.DATA_DIR) / name

        if data_dir.is_dir():
            shutil.rmtree(data_dir.as_posix())

        py_file = Path(cmd.STEP_DIR) / "data" / f"{name}.py"
        ipy_file = Path(cmd.STEP_DIR) / "data" / f"{name}.ipynb"

        if py_file.exists():
            py_file.unlink()

        if ipy_file.exists():
            ipy_file.unlink()

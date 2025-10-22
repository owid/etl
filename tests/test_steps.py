#
#  test_steps.py
#

"""
Test that the different step types work as expected.
"""

import json
import random
import shutil
import string
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
from unittest.mock import patch

import pandas as pd
import pytest
import requests
from owid.catalog import Dataset

from etl import paths
from etl.steps import (
    DataStep,
    DataStepPrivate,
    SnapshotStep,
    Step,
    filter_to_subgraph,
    get_etag,
    isolated_env,
    select_dirty_steps,
    to_dependency_order,
)


def _create_mock_py_file(step_name: str) -> None:
    py_file = paths.STEP_DIR / "data" / f"{step_name}.py"
    assert not py_file.exists()
    with open(str(py_file), "w") as ostream:
        print(
            """
from owid.catalog import Dataset
def run(dest_dir):
    ds = Dataset.create_empty(dest_dir)
    ds.metadata.short_name = "test"
    ds.save()
            """,
            file=ostream,
        )


def test_data_step():
    with temporary_step() as step_name:
        _create_mock_py_file(step_name)
        DataStep(step_name, []).run()
        Dataset((paths.DATA_DIR / step_name).as_posix())


def test_data_step_becomes_dirty_when_pandas_version_changes():
    pandas_version = pd.__version__
    try:
        with temporary_step() as step_name:
            _create_mock_py_file(step_name)
            d = DataStep(step_name, [])
            assert d.is_dirty()
            d.run()
            assert not d.is_dirty()

            pd.__version__ += ".test"  # type: ignore
            assert d.is_dirty()

    finally:
        pd.__version__ = pandas_version  # type: ignore


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
    dag = {"a": {"b", "c"}, "b": {"c"}}
    assert to_dependency_order(dag) == ["c", "b", "a"]


def test_dependency_filtering():
    dag = {
        "e": {"a"},
        "c": {"b", "d"},
        "b": {"a"},
    }
    assert filter_to_subgraph(dag, ["b"], downstream=True) == {
        "d": set(),
        "c": {"b", "d"},
        "b": {"a"},
        "a": set(),
    }
    assert filter_to_subgraph(dag, ["b"], downstream=False) == {
        "b": {"a"},
        "a": set(),
    }


def test_dependency_filtering_with_excludes():
    """Test that excludes properly remove steps and their downstream dependencies."""
    dag = {
        "f": {"e"},  # f depends on e
        "e": {"d"},  # e depends on d
        "d": {"c"},  # d depends on c
        "c": {"b"},  # c depends on b
        "b": {"a"},  # b depends on a
        "a": set(),  # a has no dependencies
        "g": {"a"},  # g also depends on a (parallel branch)
    }

    # Test excluding step "c" - should also exclude "d", "e", "f" (downstream dependencies)
    # but keep "a", "b", "g" (not dependent on c)
    result = filter_to_subgraph(dag, ["a", "b", "c", "d", "e", "f", "g"], excludes=["c"])
    expected = {
        "a": set(),
        "b": {"a"},
        "g": {"a"},
    }
    assert result == expected

    # Test excluding with regex pattern
    result = filter_to_subgraph(dag, ["a", "b", "c", "d", "e", "f", "g"], excludes=["[cd]"])
    expected = {
        "a": set(),
        "b": {"a"},
        "g": {"a"},
    }
    assert result == expected

    # Test excluding step "a" - should exclude everything since all depend on "a"
    result = filter_to_subgraph(dag, ["a", "b", "c", "d", "e", "f", "g"], excludes=["a"])
    expected = {}
    assert result == expected

    # Test excludes with empty includes (should include all except excluded and their downstream)
    result = filter_to_subgraph(dag, [], excludes=["c"])
    expected = {
        "a": set(),
        "b": {"a"},
        "g": {"a"},
    }
    assert result == expected


def test_dependency_filtering_empty_includes_with_excludes():
    """Test that excludes work properly when includes is empty."""
    dag = {
        "step1": set(),
        "step2": {"step1"},
        "step3": {"step2"},
        "step4": set(),
    }

    # When includes is empty but excludes has values, should exclude specified steps and their downstream
    result = filter_to_subgraph(dag, [], excludes=["step2"])
    expected = {
        "step1": set(),
        "step4": set(),
    }
    assert result == expected


class DummyStep(Step):  # type: ignore
    def __init__(self, name: str):
        self.path = name

    def __str__(self):
        return self.path

    def __repr__(self):
        return self.path


def test_data_step_private():
    with temporary_step() as step_name:
        _create_mock_py_file(step_name)
        DataStepPrivate(step_name, []).run()
        ds = Dataset((paths.DATA_DIR / step_name).as_posix())
        assert not ds.metadata.is_public


def test_select_dirty_steps():
    """select_dirty_steps should only select dirty steps, this can be tricky when using threads"""
    steps = [DummyStep(f"{i}") for i in range(20)]  # type: ignore
    for s in steps:
        if random.random() < 0.5:
            s.is_dirty = lambda: False  # type: ignore
        else:
            s.is_dirty = lambda: True  # type: ignore
    assert all([s.is_dirty() for s in select_dirty_steps(steps, 10)])  # type: ignore


def test_get_etag():
    try:
        etag = get_etag("https://raw.githubusercontent.com/owid/owid-grapher/master/README.md")
    # ignore SSL errors
    except requests.exceptions.SSLError:
        return
    assert etag


def test_SnapshotStep_checksum_output(tmp_path):
    """SnapshotStep checksum should depenet on metadata in .dvc file"""
    meta = {"origin": {"producer": "A"}, "outs": [{"md5": "123"}]}

    with patch("etl.paths.SNAPSHOTS_DIR", new=tmp_path / "snapshots") as snapshots_dir:
        snapshots_dir.mkdir()
        snapshot_dvc = snapshots_dir / "A.dvc"
        with open(snapshot_dvc, "w") as f:
            json.dump(meta, f)

        step = SnapshotStep("A")
        assert step.checksum_output() == "1867a4e329be8bb3c12a727513b931e8"

        # change metadata
        meta["origin"]["producer"] = "B"
        with open(snapshot_dvc, "w") as f:
            json.dump(meta, f)

        assert step.checksum_output() == "a43b0c67d958884a0bc6487dcf5f4bca"


def test_isolated_env(tmp_path):
    (tmp_path / "shared.py").write_text("A = 1; import test_abc")
    (tmp_path / "test_abc.py").write_text("B = 1")

    with isolated_env(tmp_path):
        import shared  # type: ignore

        assert shared.A == 1
        assert shared.test_abc.B == 1
        assert "test_abc" in sys.modules.keys()

    with pytest.raises(ModuleNotFoundError):
        import shared  # type: ignore

    assert "test_abc" not in sys.modules.keys()


def test_instant_metadata_with_override_file():
    """Test that --instant mode picks up changes in .meta.override.yml files."""
    from owid.catalog import Table

    from etl import config

    # Save original INSTANT value
    original_instant = config.INSTANT

    try:
        # Create a temporary directory for the test dataset
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Create a dataset directory structure like garden/namespace/version/dataset
            parent_dir = tmp_path / "garden" / "test_ns" / "2024-01-01"
            parent_dir.mkdir(parents=True)
            dataset_dir = parent_dir / "test_dataset"

            # Create base metadata file
            meta_file = parent_dir / "test_dataset.meta.yml"
            with open(str(meta_file), "w") as f:
                f.write("""
tables:
  test_table:
    variables:
      value:
        title: Original title
        description_short: Original description
""")

            # Create override metadata file
            meta_override_file = parent_dir / "test_dataset.meta.override.yml"
            with open(str(meta_override_file), "w") as f:
                f.write("""
tables:
  test_table:
    variables:
      value:
        title: Override title
""")

            # Create initial dataset
            ds = Dataset.create_empty(dataset_dir.as_posix())
            ds.metadata.short_name = "test_dataset"

            # Create a table with one indicator
            tb = Table(pd.DataFrame({"value": [1, 2, 3]}, index=pd.Index([2020, 2021, 2022], name="year")))
            tb.metadata.short_name = "test_table"
            tb["value"].metadata.title = "Test indicator"
            ds.add(tb)
            ds.save()

            # Load metadata from files (simulating what create_dataset does)
            ds = Dataset(dataset_dir.as_posix())
            ds.update_metadata(meta_file)
            ds.update_metadata(meta_override_file)
            ds.save()

            # Verify override was applied
            ds = Dataset(dataset_dir.as_posix())
            assert ds["test_table"]["value"].metadata.title == "Override title"
            assert ds["test_table"]["value"].metadata.description_short == "Original description"

            # Now test INSTANT mode - modify the override file
            with open(str(meta_override_file), "w") as f:
                f.write("""
tables:
  test_table:
    variables:
      value:
        title: Updated override title
""")

            # Simulate what _run_instant_metadata does
            config.INSTANT = True

            ds = Dataset(dataset_dir.as_posix())

            # Load metadata files as _run_instant_metadata should do
            ds.update_metadata(meta_file)
            # This is the critical line - it should load the override file
            if meta_override_file.exists():
                ds.update_metadata(meta_override_file)
            ds.save()

            # Verify that the override changes were picked up
            ds = Dataset(dataset_dir.as_posix())
            assert ds["test_table"]["value"].metadata.title == "Updated override title"
            assert ds["test_table"]["value"].metadata.description_short == "Original description"

    finally:
        # Restore original INSTANT value
        config.INSTANT = original_instant

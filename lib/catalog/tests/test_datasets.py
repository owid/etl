#
#  test_datasets.py
#

import json
import os
import random
import shutil
import tempfile
from contextlib import contextmanager
from glob import glob
from os import rmdir
from os.path import exists, join
from pathlib import Path
from typing import Any, Iterator, Optional, Union
from unittest.mock import patch

import pandas as pd
import pytest
import yaml

from owid.catalog import Dataset, DatasetMeta, Table
from owid.catalog.datasets import NonUniqueIndex, PrimaryKeyMissing

from .mocking import mock
from .test_tables import mock_table


def test_dataset_fails_to_load_empty_folder():
    with temp_dataset_dir(create=True) as dirname:
        with pytest.raises(Exception):
            Dataset(dirname)


def test_create_empty():
    with temp_dataset_dir(create=True) as dirname:
        shutil.rmtree(dirname)

        ds = Dataset.create_empty(dirname)

        assert exists(join(dirname, "index.json"))
        with open(join(dirname, "index.json")) as istream:
            doc = json.load(istream)
        assert doc == {"is_public": True}

        assert len(ds.index()) == 0


def test_create_empty_with_metadata(tmpdir):
    ds = Dataset.create_empty(tmpdir / "dataset", DatasetMeta(namespace="test"))
    assert ds.metadata.namespace == "test"


def test_create_fails_if_non_dataset_dir_exists():
    with temp_dataset_dir(create=True) as dirname:
        with pytest.raises(Exception):
            Dataset.create_empty(dirname)


def test_create_overwrites_entire_folder():
    with temp_dataset_dir(create=True) as dirname:
        with open(join(dirname, "index.json"), "w") as ostream:
            ostream.write('{"clam": "chowder"}')

        with open(join(dirname, "hallo-thar.txt"), "w") as ostream:
            ostream.write("Hello")

    d = Dataset.create_empty(dirname)

    # this should have been deleted
    assert not exists(join(dirname, "hallo-thar.txt"))

    assert open(d._index_file).read().strip() == '{\n  "is_public": true\n}'


def test_add_table():
    t = mock_table()

    with temp_dataset_dir() as dirname:
        # make a dataset
        ds = Dataset.create_empty(dirname)
        ds.metadata = DatasetMeta(short_name="bob")

        # add the table, it should be on disk now
        ds.add(t)
        assert t.metadata.dataset == ds.metadata

        # check that it's really on disk
        table_files = [
            join(dirname, t.metadata.checked_name + ".feather"),
            join(dirname, t.metadata.checked_name + ".meta.json"),
        ]
        for filename in table_files:
            assert exists(filename)

        # check other methods on Dataset
        assert len(ds) == 1
        assert len(ds.index()) == 1
        assert t.metadata.checked_name in ds

        # load a fresh copy from disk
        t2 = ds[t.metadata.checked_name]
        assert id(t2) != id(t)

        # the fresh copy from disk should be identical to the copy we added
        assert t2.metadata.primary_key == t.metadata.primary_key
        assert t2.equals_table(t)
        assert t2.metadata.dataset == ds.metadata


@patch.dict(os.environ, {})
def test_add_table_without_primary_key():
    t = mock_table().reset_index()

    with temp_dataset_dir() as dirname:
        ds = Dataset.create_empty(dirname)
        with pytest.warns(UserWarning):
            ds.add(t)


@patch.dict(os.environ, {"OWID_STRICT": "1"})
def test_add_table_without_primary_key_strict_mode():
    t = mock_table().reset_index()

    with temp_dataset_dir() as dirname:
        ds = Dataset.create_empty(dirname)
        with pytest.raises(PrimaryKeyMissing):
            ds.add(t)


@patch.dict(os.environ, {"OWID_STRICT": "1"})
def test_add_table_without_unique_index():
    t = Table(pd.DataFrame({"a": [1, 1], "b": [1, 2]}).set_index("a"))

    with temp_dataset_dir() as dirname:
        ds = Dataset.create_empty(dirname)
        with pytest.raises(NonUniqueIndex):
            ds.add(t)


def test_add_table_csv():
    t = mock_table()

    with temp_dataset_dir() as dirname:
        # make a dataset
        ds = Dataset.create_empty(dirname)

        # add the table, it should be on disk now
        ds.add(t, formats=["csv"])

        # check that it's really on disk
        table_files = [
            join(dirname, t.metadata.checked_name + ".csv"),
            join(dirname, t.metadata.checked_name + ".meta.json"),
        ]
        for filename in table_files:
            assert exists(filename)

        # load a fresh copy from disk
        t2 = ds[t.metadata.checked_name]
        assert id(t2) != id(t)

        # the fresh copy from disk should be identical to the copy we added
        assert t2.equals_table(t)


def test_add_table_parquet():
    t = mock_table()

    with temp_dataset_dir() as dirname:
        # make a dataset
        ds = Dataset.create_empty(dirname)

        # add the table, it should be on disk now
        ds.add(t, formats=["parquet"])

        # check that it's really on disk
        assert exists(join(dirname, t.metadata.checked_name + ".parquet"))

        # metadata exists as a sidecar JSON
        assert exists(join(dirname, t.metadata.checked_name + ".meta.json"))

        # load a fresh copy from disk
        t2 = ds[t.metadata.checked_name]
        assert id(t2) != id(t)

        # the fresh copy from disk should be identical to the copy we added
        assert t2.equals_table(t)


def test_metadata_roundtrip():
    with temp_dataset_dir() as dirname:
        d = Dataset.create_empty(dirname)
        d.metadata = mock(DatasetMeta)
        d.save()

        d2 = Dataset(dirname)
        assert d2.metadata == d.metadata


def test_dataset_size():
    with mock_dataset() as d:
        n_expected = len(glob(join(d.path, "*.feather")))
        assert len(d) == n_expected


def test_dataset_iteration():
    with mock_dataset() as d:
        i = 0
        for table in d:
            i += 1
        assert i == len(d)


def test_dataset_hash_changes_with_data_changes():
    with mock_dataset() as d:
        c1 = d.checksum()

        t = mock_table()
        d.add(t)
        c2 = d.checksum()

        assert c1 != c2


def test_dataset_hash_invariant_to_copying():
    # make a mock dataset
    with mock_dataset() as d1:
        # make a copy of it
        with temp_dataset_dir() as dirname:
            d2 = Dataset.create_empty(dirname)
            d2.metadata = d1.metadata
            d2.save()

            for t in d1:
                d2.add(t)

            # the copy should have the same checksum
            assert d2.checksum() == d1.checksum()


def test_snake_case_dataset():
    with mock_dataset() as d:
        # short_name of a dataset must be snake_case
        d.metadata.short_name = "camelCase"
        with pytest.raises(NameError):
            d.save()


def test_snake_case_table():
    with mock_dataset() as d:
        # short_name of a table must be snake_case
        t = mock_table()
        t.metadata.short_name = "camelCase"
        with pytest.raises(NameError):
            d.add(t)

        # short_name of a dataset must be snake_case
        t = mock_table()
        t["camelCase"] = 1
        with pytest.raises(NameError):
            d.add(t)

        # short_name of columns and index names must be snake_case
        t = mock_table()
        t.index.names = ["Country"]
        with pytest.raises(NameError):
            d.add(t)


def test_update_metadata(tmp_path):
    with mock_dataset() as d:
        table_name = d.table_names[0]

        # create test yml file
        temp_file = tmp_path / "my.meta.yml"
        meta = {
            "dataset": {"title": "Dataset title from YAML"},
            "tables": {table_name: {"variables": {"gdp": {"title": "Variable title from YAML"}}}},
        }
        temp_file.write_text(yaml.dump(meta))

        d.update_metadata(temp_file)

        assert d.metadata.title == "Dataset title from YAML"
        assert d[table_name]["gdp"].metadata.title == "Variable title from YAML"


def test_bool():
    with mock_dataset(n_tables=0) as d:
        assert bool(d)


def test_save_fills_channel(tmp_path: Path):
    path = tmp_path / "garden/owid/latest/shortname"
    path.parent.mkdir(exist_ok=True, parents=True)

    d = Dataset.create_empty(path)
    d.metadata = mock(DatasetMeta)
    d.save()

    d2 = Dataset(path)
    assert d2.metadata.channel == "garden"


def test_save_without_valid_channel(tmp_path: Path):
    path = tmp_path / "invalid/owid/latest/shortname"
    path.parent.mkdir(exist_ok=True, parents=True)

    d = Dataset.create_empty(path)
    d.metadata = mock(DatasetMeta)
    d.metadata.channel = None
    d.save()

    d2 = Dataset(path)
    assert d2.metadata.channel is None


@contextmanager
def temp_dataset_dir(create: bool = False) -> Iterator[str]:
    with tempfile.TemporaryDirectory() as dirname:
        if not create:
            rmdir(dirname)
        yield dirname


def create_temp_dataset(dirname: Union[Path, str], n_tables: Optional[int] = None) -> Dataset:
    d = Dataset.create_empty(dirname)
    d.metadata = mock(DatasetMeta)
    d.metadata.version = random.choice(["latest", "2023-01-01"])
    d.metadata.short_name = Path(dirname).name
    d.metadata.is_public = True
    d.save()

    if n_tables is None:
        n_tables = random.randint(2, 5)

    for _ in range(n_tables):
        t = mock_table()
        d.add(t)
    return d


@contextmanager
def mock_dataset(**kwargs: Any) -> Iterator[Dataset]:
    with temp_dataset_dir() as dirname:
        d = create_temp_dataset(dirname, **kwargs)
        yield d

#
#  test_datasets.py
#

import tempfile
from os.path import join, exists
from os import rmdir
import json
import shutil
from contextlib import contextmanager
from typing import Iterator

import pytest

from etl.datasets import Dataset
from etl.meta import DatasetMeta
from .test_tables import mock_table
from .mocking import mock


def test_dataset_fails_to_load_empty_folder():
    with temp_dataset_dir(create=True) as dirname:
        with pytest.raises(Exception):
            Dataset(dirname)


def test_create_empty():
    with temp_dataset_dir(create=True) as dirname:
        shutil.rmtree(dirname)

        Dataset.create_empty(dirname)

        assert exists(join(dirname, "index.json"))
        with open(join(dirname, "index.json")) as istream:
            doc = json.load(istream)
        assert doc == {}


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

    assert open(d._index_file).read().strip() == "{}"


def test_add_table():
    t = mock_table()

    with temp_dataset_dir() as dirname:
        # make a dataset
        ds = Dataset.create_empty(dirname)

        # add the table, it should be on disk now
        ds.add(t)

        # check that it's really on disk
        table_files = [
            join(dirname, t.name + ".feather"),
            join(dirname, t.name + ".meta.json"),
        ]
        for filename in table_files:
            assert exists(filename)

        # load a fresh copy from disk
        t2 = ds[t.name]
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


@contextmanager
def temp_dataset_dir(create: bool = False) -> Iterator[str]:
    with tempfile.TemporaryDirectory() as dirname:
        if not create:
            rmdir(dirname)
        yield dirname

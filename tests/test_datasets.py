#
#  test_datasets.py
#

import tempfile
from os.path import join, exists
from os import rmdir
import json
import shutil

import pytest

from etl.datasets import Dataset, DatasetMeta
from .test_tables import mock_table


def test_dataset_fails_to_load_empty_folder():
    with tempfile.TemporaryDirectory() as dirname:
        with pytest.raises(Exception):
            Dataset(dirname)


def test_create_empty():
    with tempfile.TemporaryDirectory() as dirname:
        shutil.rmtree(dirname)

        Dataset.create_empty(dirname)

        assert exists(join(dirname, "index.json"))
        with open(join(dirname, "index.json")) as istream:
            doc = json.load(istream)
        assert doc == {}


def test_create_fails_if_non_dataset_dir_exists():
    with tempfile.TemporaryDirectory() as dirname:
        with pytest.raises(Exception):
            Dataset.create_empty(dirname)


def test_create_overwrites_entire_folder():
    with tempfile.TemporaryDirectory() as dirname:
        with open(join(dirname, "index.json"), "w") as ostream:
            ostream.write('{"clam": "chowder"}')

        with open(join(dirname, "hallo-thar.txt"), "w") as ostream:
            ostream.write("Hello")

    d = Dataset.create_empty(dirname)

    # this should have been deleted
    assert not exists(join(dirname, "hallo-thar.txt"))

    assert open(d._index_file).read().strip() == "{}"


def test_loading_metadata_does_not_trigger_autosaving():
    with tempfile.NamedTemporaryFile() as temp:
        with open(temp.name, "w") as ostream:
            json.dump({"title": "Hello", "description": "Thar"}, ostream)

        metadata = DatasetMeta.load(temp.name)
        assert metadata._save_count == 0
        assert metadata.title == "Hello"
        assert metadata.description == "Thar"

        metadata.title = "I changed my mind"
        assert metadata._save_count == 1


def test_add_table():
    t = mock_table()

    with tempfile.TemporaryDirectory() as dirname:
        rmdir(dirname)

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

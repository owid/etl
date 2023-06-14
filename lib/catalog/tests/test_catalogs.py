#
#  test_catalogs.py
#

import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator, Optional

import pytest  # noqa

from owid.catalog import CHANNEL, LocalCatalog, RemoteCatalog, Table, find

from .test_datasets import create_temp_dataset

_catalog: Optional[RemoteCatalog] = None


def load_catalog() -> RemoteCatalog:
    global _catalog

    if _catalog is None:
        _catalog = RemoteCatalog()

    return _catalog


def test_remote_catalog_loads():
    load_catalog()


def test_remote_find_returns_all():
    c = load_catalog()
    assert len(c.find()) == len(c.frame)


def test_remote_find_one():
    c = load_catalog()
    t = c.find_one("population_density", dataset="key_indicators", namespace="owid")
    assert isinstance(t, Table)


def test_remote_getitem():
    c = load_catalog()
    t = c["garden/owid/latest/key_indicators/population_density"]
    assert isinstance(t, Table)


def test_remote_default_channel():
    c = load_catalog()
    assert set(c.frame.channel) == {"garden"}


def test_find_from_local_catalog():
    with mock_catalog(3) as catalog:
        matches = catalog.find()
        assert len(matches.dataset.unique()) == 3


def test_getitem_from_local_catalog():
    with mock_catalog(1) as catalog:
        path = catalog.find().iloc[0].path
        t = catalog[path]
        assert isinstance(t, Table)


def test_load_from_local_catalog():
    with mock_catalog(1) as catalog:
        catalog.find().iloc[0].load()


def test_local_default_channel():
    with mock_catalog(1, channels=("open_numbers",)) as catalog:
        catalog.find()

    with mock_catalog(1, channels=("garden", "meadow")) as catalog:
        assert set(catalog.find().channel) == {"garden", "meadow"}


def test_calling_find_adds_channels():
    find("abc")
    from owid.catalog.catalogs import REMOTE_CATALOG

    assert REMOTE_CATALOG.channels == ("garden",)  # type: ignore

    find("abc", channels=("garden", "meadow"))
    from owid.catalog.catalogs import REMOTE_CATALOG

    assert set(REMOTE_CATALOG.channels) == {"garden", "meadow"}  # type: ignore


def test_reindex_with_include():
    with mock_catalog(3, channels=("garden",)) as catalog:
        old_frame = catalog.frame.copy()

        # create new dataset0
        create_temp_dataset(catalog.path / "garden" / "dataset0")

        # reindex
        catalog.reindex(include="dataset0")
        new_frame = catalog.frame

        # and make sure we have a new checksum for dataset0
        assert set(old_frame[old_frame.dataset == "dataset0"].checksum) != set(
            new_frame[new_frame.dataset == "dataset0"].checksum
        )

        # and same checksum for others
        assert set(old_frame[old_frame.dataset != "dataset0"].checksum) == set(
            new_frame[new_frame.dataset != "dataset0"].checksum
        )


@contextmanager
def mock_catalog(n: int = 3, channels: Iterable[CHANNEL] = ("garden",)) -> Iterator[LocalCatalog]:
    with tempfile.TemporaryDirectory() as dirname:
        path = Path(dirname)
        for channel in channels:
            (path / channel).mkdir()
            for i in range(n):
                create_temp_dataset(path / channel / f"dataset{i}")
        yield LocalCatalog(path, channels=channels)

#
#  test_catalogs.py
#

import tempfile
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest  # noqa

from owid.catalog import CHANNEL, LocalCatalog, RemoteCatalog, Table, find

from .test_datasets import create_temp_dataset

_catalog: RemoteCatalog | None = None


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


def test_find_case_insensitive():
    """Test that find() is case-insensitive by default."""
    with mock_catalog(3) as catalog:
        # Should match "dataset0" with uppercase search
        matches = catalog.find(dataset="DATASET0")
        assert len(matches) > 0
        assert set(matches.dataset) == {"dataset0"}

        # Should also match with mixed case
        matches = catalog.find(dataset="DaTaSeT1")
        assert len(matches) > 0
        assert set(matches.dataset) == {"dataset1"}


def test_find_case_sensitive():
    """Test that find() can be made case-sensitive."""
    with mock_catalog(3) as catalog:
        # Should NOT match with case=True
        matches = catalog.find(dataset="DATASET0", case=True)
        assert len(matches) == 0

        # Should match exact case
        matches = catalog.find(dataset="dataset0", case=True)
        assert len(matches) > 0
        assert set(matches.dataset) == {"dataset0"}


def test_find_regex():
    """Test that find() supports regex patterns by default."""
    with mock_catalog(3) as catalog:
        # Match multiple datasets with regex
        matches = catalog.find(dataset="dataset[01]")
        assert len(matches) > 0
        assert set(matches.dataset) == {"dataset0", "dataset1"}

        # Match with wildcard
        matches = catalog.find(dataset="data.*2")
        assert len(matches) > 0
        assert set(matches.dataset) == {"dataset2"}


def test_find_regex_disabled():
    """Test that regex can be disabled for literal matching."""
    with mock_catalog(3) as catalog:
        # With regex=False, special chars are treated literally
        matches = catalog.find(dataset="dataset[01]", regex=False)
        assert len(matches) == 0

        # Literal match still works
        matches = catalog.find(dataset="dataset0", regex=False)
        assert len(matches) > 0
        assert set(matches.dataset) == {"dataset0"}


def test_find_fuzzy():
    """Test that find() supports fuzzy matching."""
    with mock_catalog(3) as catalog:
        # Fuzzy match with typo - "datset0" should match "dataset0"
        # Use high threshold to only match the specific one
        matches = catalog.find(dataset="datset0", fuzzy=True, threshold=80)
        assert len(matches) > 0
        assert "dataset0" in set(matches.dataset)

        # Exact match should also work with fuzzy=True
        matches = catalog.find(dataset="dataset1", fuzzy=True, threshold=100)
        assert len(matches) > 0
        assert set(matches.dataset) == {"dataset1"}


def test_find_fuzzy_threshold():
    """Test that fuzzy threshold controls match strictness."""
    with mock_catalog(3) as catalog:
        # High threshold - only very close matches
        matches_strict = catalog.find(dataset="datset", fuzzy=True, threshold=90)

        # Low threshold - more permissive
        matches_permissive = catalog.find(dataset="datset", fuzzy=True, threshold=50)

        # Permissive should match at least as many as strict
        assert len(matches_permissive) >= len(matches_strict)


def test_find_fuzzy_case_insensitive():
    """Test that fuzzy matching respects case parameter."""
    with mock_catalog(3) as catalog:
        # Case-insensitive (default) - exact match with different case
        matches = catalog.find(dataset="DATASET0", fuzzy=True, threshold=100)
        assert len(matches) > 0
        assert set(matches.dataset) == {"dataset0"}

        # Case-sensitive - should not match uppercase against lowercase
        matches = catalog.find(dataset="DATASET0", fuzzy=True, case=True, threshold=100)
        assert len(matches) == 0


def test_find_fuzzy_sorted_by_score():
    """Test that fuzzy results are sorted by match score (best first)."""
    with mock_catalog(3) as catalog:
        # Search with low threshold to get multiple matches
        matches = catalog.find(dataset="dataset0", fuzzy=True, threshold=50)
        assert len(matches) > 0

        # First result should be the exact match (dataset0)
        assert matches.iloc[0].dataset == "dataset0"


@contextmanager
def mock_catalog(n: int = 3, channels: Iterable[CHANNEL] = ("garden",)) -> Iterator[LocalCatalog]:
    with tempfile.TemporaryDirectory() as dirname:
        path = Path(dirname)
        for channel in channels:
            (path / channel).mkdir()
            for i in range(n):
                create_temp_dataset(path / channel / f"dataset{i}")
        yield LocalCatalog(path, channels=channels)

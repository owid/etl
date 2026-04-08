import tempfile
import zipfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from owid.catalog import Origin

from etl.snapshot import SnapshotArchive, SnapshotMeta, _parse_snapshot_path


@pytest.fixture
def test_archive_path():
    """Create a test zip archive with nested folder structure."""
    with tempfile.TemporaryDirectory() as tmpdir:
        archive_path = Path(tmpdir) / "test_archive.zip"

        # Create a zip file with nested structure
        with zipfile.ZipFile(archive_path, "w") as zf:
            # Root level files
            zf.writestr("root_file.csv", "col1,col2\n1,2\n3,4")
            zf.writestr("readme.txt", "This is a readme")

            # Nested data folder
            zf.writestr("data/2020.csv", "year,value\n2020,100")
            zf.writestr("data/2021.csv", "year,value\n2021,200")
            zf.writestr("data/nested/deep.csv", "a,b\n1,2")

            # Metadata folder
            zf.writestr("meta/info.json", '{"name": "test"}')

        yield archive_path


@pytest.fixture
def mock_snapshot():
    """Create a mock Snapshot object for testing."""
    snapshot = MagicMock()
    snapshot.to_table_metadata.return_value = MagicMock()
    snapshot.metadata.origin = None
    return snapshot


@pytest.fixture
def extracted_archive(test_archive_path, mock_snapshot):
    """Create a SnapshotArchive from the test archive."""
    with tempfile.TemporaryDirectory() as extract_dir:
        import zipfile

        with zipfile.ZipFile(test_archive_path, "r") as zf:
            zf.extractall(extract_dir)
        yield SnapshotArchive(mock_snapshot, Path(extract_dir))


class TestSnapshotArchive:
    """Tests for the SnapshotArchive class."""

    def test_files_returns_sorted_list(self, extracted_archive):
        """Test that archive.files returns all files sorted."""
        files = extracted_archive.files
        expected = [
            "data/2020.csv",
            "data/2021.csv",
            "data/nested/deep.csv",
            "meta/info.json",
            "readme.txt",
            "root_file.csv",
        ]
        assert files == expected

    def test_files_cached(self, extracted_archive):
        """Test that files list is cached."""
        files1 = extracted_archive.files
        files2 = extracted_archive.files
        assert files1 is files2  # Same object, not recomputed

    def test_glob_root_level(self, extracted_archive):
        """Test glob pattern matching at root level."""
        csv_files = extracted_archive.glob("*.csv")
        assert csv_files == ["root_file.csv"]

    def test_glob_recursive(self, extracted_archive):
        """Test recursive glob pattern matching."""
        all_csv = extracted_archive.glob("**/*.csv")
        assert all_csv == [
            "data/2020.csv",
            "data/2021.csv",
            "data/nested/deep.csv",
            "root_file.csv",
        ]

    def test_glob_specific_folder(self, extracted_archive):
        """Test glob in specific folder."""
        data_files = extracted_archive.glob("data/*.csv")
        assert data_files == ["data/2020.csv", "data/2021.csv"]

    def test_glob_nested_folder(self, extracted_archive):
        """Test glob in nested folder."""
        nested_files = extracted_archive.glob("data/nested/*")
        assert nested_files == ["data/nested/deep.csv"]

    def test_glob_no_matches(self, extracted_archive):
        """Test glob with no matches returns empty list."""
        assert extracted_archive.glob("*.xlsx") == []

    def test_contains_existing_file(self, extracted_archive):
        """Test 'in' operator for existing file."""
        assert "root_file.csv" in extracted_archive
        assert "data/2020.csv" in extracted_archive
        assert "data/nested/deep.csv" in extracted_archive

    def test_contains_missing_file(self, extracted_archive):
        """Test 'in' operator for missing file."""
        assert "nonexistent.csv" not in extracted_archive
        assert "data/missing.csv" not in extracted_archive

    def test_path_property(self, extracted_archive):
        """Test path property returns extraction directory."""
        path = extracted_archive.path
        assert isinstance(path, Path)
        assert path.is_dir()

    def test_read_missing_file_error_message(self, extracted_archive):
        """Test that reading missing file shows helpful error with available files."""
        with pytest.raises(FileNotFoundError) as exc_info:
            extracted_archive.read("nonexistent.csv")

        error_message = str(exc_info.value)
        assert "nonexistent.csv" in error_message
        assert "not found in archive" in error_message
        assert "Available files:" in error_message
        # Should list some of the available files
        assert "data/2020.csv" in error_message
        assert "root_file.csv" in error_message


def test_parse_snapshot_path():
    path = Path("etl/snapshots/aviation_safety_network/2023-04-18/aviation_statistics_by_period.csv.dvc")
    assert _parse_snapshot_path(path) == (
        "aviation_safety_network",
        "2023-04-18",
        "aviation_statistics_by_period",
        "csv",
    )

    # snapshot names shouldn't contain dot
    with pytest.raises(AssertionError):
        path = Path("etl/snapshots/unep/2023-03-17/consumption_controlled_substances.hydrobromofluorocarbons.xlsx.dvc")
        _parse_snapshot_path(path)


def test_snapshot_to_yaml():
    d = SnapshotMeta(
        namespace="aviation_safety_network",
        version="2023-04-18",
        short_name="aviation_statistics_by_period",
        file_extension="csv",
        origin=Origin(producer="Producer", title="Aviation Statistics by Period"),
    ).to_dict()
    assert d == {
        "file_extension": "csv",
        "is_public": True,
        "namespace": "aviation_safety_network",
        "short_name": "aviation_statistics_by_period",
        "version": "2023-04-18",
        "origin": {"title": "Aviation Statistics by Period", "producer": "Producer"},
    }

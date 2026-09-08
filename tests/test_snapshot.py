import tempfile
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from owid.catalog import Origin, s3_utils

from etl import config, paths
from etl.files import checksum_file, ruamel_load
from etl.snapshot import Snapshot, SnapshotArchive, SnapshotMeta, _parse_snapshot_path


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


DVC_TEMPLATE = """meta:
  origin:
    producer: Producer
    title: Test dataset
    date_published: "2024-01-01"
    url_main: https://example.com
    date_accessed: "2024-01-02"
    license:
      name: CC BY 4.0
      url: https://example.com/license
outs:
  - md5: {md5}
    size: 1
    path: test.csv
"""

# md5 of the data file written by the `snapshot` fixture.
DATA_MD5 = "e5ebd4c02cefbe7955977c67ada242b7"


@pytest.fixture
def snapshot(monkeypatch):
    """A Snapshot backed by a temporary snapshots/ and data/ tree, whose .dvc records a stale md5."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        monkeypatch.setattr(paths, "SNAPSHOTS_DIR", tmpdir / "snapshots")
        monkeypatch.setattr(paths, "DATA_DIR", tmpdir / "data")

        dvc_path = paths.SNAPSHOTS_DIR / "ns/2024-01-01/test.csv.dvc"
        dvc_path.parent.mkdir(parents=True)
        dvc_path.write_text(DVC_TEMPLATE.format(md5="0" * 32))

        data_path = paths.DATA_DIR / "snapshots/ns/2024-01-01/test.csv"
        data_path.parent.mkdir(parents=True)
        data_path.write_text("a,b\n1,2\n")

        yield Snapshot("ns/2024-01-01/test.csv")


class TestDvcAdd:
    """Tests for Snapshot.dvc_add, which must never re-upload an object already on R2.

    Snapshot keys are content-addressed, and the bucket lock policy on owid-snapshots rejects
    overwrites with ObjectLockedByBucketPolicy.
    """

    @pytest.fixture(autouse=True)
    def uploads(self, monkeypatch):
        """Record calls to s3_utils.upload instead of hitting R2."""
        calls = []
        monkeypatch.setattr("etl.snapshot.s3_utils.upload", lambda *args, **kwargs: calls.append(args))
        return calls

    def test_uploads_when_missing_from_remote(self, snapshot, uploads, monkeypatch):
        monkeypatch.setattr(Snapshot, "_snapshot_exists_on_remote", lambda self, md5: False)

        snapshot.dvc_add(upload=True)

        assert len(uploads) == 1
        assert uploads[0][0] == f"s3://owid-snapshots/{DATA_MD5[:2]}/{DATA_MD5[2:]}"
        assert ruamel_load(snapshot.metadata_path.read_text())["outs"][0]["md5"] == DATA_MD5

    def test_skips_upload_when_already_on_remote_but_dvc_is_stale(self, snapshot, uploads, monkeypatch):
        """The autoupdate case: the new md5 only exists on an open PR branch, so the local .dvc
        still holds master's older md5 even though the file is already on R2."""
        monkeypatch.setattr(Snapshot, "_snapshot_exists_on_remote", lambda self, md5: True)

        snapshot.dvc_add(upload=True)

        assert uploads == []
        # The .dvc is still brought up to date, otherwise the snapshot would never be recorded.
        assert ruamel_load(snapshot.metadata_path.read_text())["outs"][0]["md5"] == DATA_MD5

    def test_no_op_when_dvc_matches_and_file_is_on_remote(self, snapshot, uploads, monkeypatch):
        snapshot.metadata_path.write_text(DVC_TEMPLATE.format(md5=DATA_MD5))
        monkeypatch.setattr(Snapshot, "_snapshot_exists_on_remote", lambda self, md5: True)
        before = snapshot.metadata_path.read_text()

        snapshot.dvc_add(upload=True)

        assert uploads == []
        # Nothing changed, so the .dvc is left untouched rather than round-tripped through ruamel.
        assert snapshot.metadata_path.read_text() == before

    def test_reuploads_when_dvc_matches_but_file_is_missing_from_remote(self, snapshot, uploads, monkeypatch):
        snapshot.metadata_path.write_text(DVC_TEMPLATE.format(md5=DATA_MD5))
        monkeypatch.setattr(Snapshot, "_snapshot_exists_on_remote", lambda self, md5: False)

        snapshot.dvc_add(upload=True)

        assert len(uploads) == 1

    def test_skips_everything_without_upload(self, snapshot, uploads, monkeypatch):
        monkeypatch.setattr(Snapshot, "_snapshot_exists_on_remote", lambda self, md5: False)

        snapshot.dvc_add(upload=False)

        assert uploads == []


# The snapshot the autoupdate build kept failing on: it is updated daily behind a long-lived PR, so
# its local .dvc regularly holds an older md5 than the object already stored on R2.
EPOCH_URI = "artificial_intelligence/2025-03-12/epoch_compute_intensive.csv"


@pytest.mark.integration
def test_dvc_add_skips_upload_of_snapshot_already_on_r2():
    """Against real R2 and the real producer: never re-upload a snapshot that is already stored.

    Reproduces the state that made the autoupdate build fail with ObjectLockedByBucketPolicy — the
    downloaded content is unchanged, so its md5 is already on R2, but the local .dvc still holds an
    older md5 and therefore can't be used to detect that.

    Skips rather than passes when that state can't be reached, so a skip is never mistaken for a
    verified run. Downloads into data/snapshots/ and restores the .dvc afterwards; never writes to
    R2 — a genuine upload would need a brand new object in a locked production bucket.
    """
    snap = Snapshot(EPOCH_URI)
    snap.download_from_source()

    md5 = checksum_file(snap.path)
    assert snap.metadata.outs is not None
    if md5 == snap.metadata.outs[0]["md5"]:
        pytest.skip(f"local .dvc already records the current content ({md5}), nothing to reproduce")
    if not s3_utils.object_exists(f"s3://{config.R2_SNAPSHOTS_PUBLIC}/{md5[:2]}/{md5[2:]}"):
        pytest.skip(f"content is new ({md5}), so dvc_add should upload it; not writing to production R2")

    original_dvc = snap.metadata_path.read_text()
    try:
        with patch.object(s3_utils, "upload", side_effect=AssertionError("re-uploaded an object already on R2")):
            snap.dvc_add(upload=True)

        # The .dvc must still be brought up to date, or the snapshot would never be recorded.
        # Checked on the file rather than snap.metadata, which is only read at construction.
        assert ruamel_load(snap.metadata_path.read_text())["outs"][0]["md5"] == md5
    finally:
        snap.metadata_path.write_text(original_dvc)


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

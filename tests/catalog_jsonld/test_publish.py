from pathlib import Path
from typing import Any

import pandas as pd
from botocore.client import ClientError
from owid.catalog import Dataset, DatasetMeta, License, Origin, Table, VariableMeta
from owid.catalog.api.legacy import LocalCatalog

from etl.catalog_jsonld.artifacts import DATASET_JSONLD_FILENAME, QUALITY_REPORT_FILENAME, SITEMAP_FILENAME
from etl.catalog_jsonld.publish import build_and_publish_catalog_jsonld, sync_jsonld_artifacts


def _step_uri(catalog_path: str) -> str:
    """Convert a catalog dataset path (e.g. 'garden/ns/2025-01-01/short_name') to its DAG step URI."""
    return f"data://{catalog_path}"


class FakeS3:
    """Minimal stand-in for the boto3 S3 client used by sync_jsonld_artifacts."""

    def __init__(self, remote_md5: dict[str, str] | None = None) -> None:
        self.remote_md5 = dict(remote_md5 or {})
        self.uploaded: list[str] = []
        self.deleted: list[str] = []

    def head_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        if Key not in self.remote_md5:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
        return {"ETag": f'"{self.remote_md5[Key]}"'}

    def upload_file(self, Filename: str, Bucket: str, Key: str, ExtraArgs: dict[str, Any] | None = None) -> None:
        self.uploaded.append(Key)

    def delete_object(self, Bucket: str, Key: str) -> None:
        self.deleted.append(Key)


def _add_eligible_dataset(
    data_dir: Path,
    *,
    namespace: str,
    dataset: str,
    version: str = "2025-01-01",
    non_redistributable: bool = False,
) -> str:
    dataset_dir = data_dir / "garden" / namespace / version / dataset
    origin = Origin(
        producer="Example Producer",
        title="Original dataset",
        description="Original description",
        citation_full="Example Producer (2025). Original dataset.",
        license=License(name="CC BY 4.0"),
    )
    ds = Dataset.create_empty(
        dataset_dir,
        DatasetMeta(
            channel="garden",
            namespace=namespace,
            version=version,
            short_name=dataset,
            title=f"{dataset} title",
            description=f"{dataset} description",
            non_redistributable=non_redistributable,
        ),
    )
    tb = Table(pd.DataFrame({"year": [2020], "value": [1]}), short_name="example_table")
    tb = tb.set_index("year")
    tb.metadata.title = "Example table"
    tb.metadata.description = "Table description"
    tb._fields["value"] = VariableMeta(title="Value", description_short="A measured value.", origins=[origin])
    ds.add(tb)
    ds.save()
    return f"garden/{namespace}/{version}/{dataset}"


def test_sync_jsonld_artifacts_uploads_new_and_deletes_stale(tmp_path: Path) -> None:
    catalog_dir = tmp_path
    new_key = "emissions/owid_co2/dataset.jsonld"
    (catalog_dir / "emissions" / "owid_co2").mkdir(parents=True)
    (catalog_dir / "emissions" / "owid_co2" / "dataset.jsonld").write_text('{"a": 1}')

    unchanged_key = "wb/world_bank_pip/dataset.jsonld"
    (catalog_dir / "wb" / "world_bank_pip").mkdir(parents=True)
    unchanged_path = catalog_dir / "wb" / "world_bank_pip" / "dataset.jsonld"
    unchanged_path.write_text('{"b": 2}')

    from etl import files

    unchanged_md5 = files.checksum_file(unchanged_path)
    stale_key = "garden/emissions/2025-12-04/owid_co2/dataset.jsonld"
    # The stale key must actually exist on "remote" for a delete to be meaningful/observable.
    s3 = FakeS3(remote_md5={unchanged_key: unchanged_md5, stale_key: "deadbeefdeadbeefdeadbeefdeadbeef"})

    sync_jsonld_artifacts(
        s3, "test-bucket", catalog_dir, [new_key, unchanged_key], delete_keys=[stale_key, unchanged_key]
    )

    assert s3.uploaded == [new_key]
    # unchanged_key matches the remote checksum, so it's neither re-uploaded nor deleted (it's
    # also excluded from delete_keys because it's present in `keys`).
    assert stale_key in s3.deleted
    assert unchanged_key not in s3.deleted


def test_sync_jsonld_artifacts_skips_delete_for_local_file_not_on_remote(tmp_path: Path) -> None:
    s3 = FakeS3(remote_md5={})
    sync_jsonld_artifacts(s3, "test-bucket", tmp_path, [], delete_keys=["never/existed/dataset.jsonld"])
    assert s3.deleted == []


def test_build_and_publish_catalog_jsonld_uses_short_keys_and_deletes_old_dated_paths(
    tmp_path: Path, monkeypatch
) -> None:
    data_dir = tmp_path / "data"
    co2_path = _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2")
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    captured: dict[str, Any] = {}

    def fake_sync_jsonld_artifacts(s3, bucket, catalog_dir, keys, delete_keys=None):
        captured["keys"] = keys
        captured["delete_keys"] = delete_keys

    monkeypatch.setattr("etl.catalog_jsonld.publish.connect_r2", lambda: object())
    monkeypatch.setattr("etl.catalog_jsonld.publish.sync_jsonld_artifacts", fake_sync_jsonld_artifacts)

    build_and_publish_catalog_jsonld(
        bucket="test-bucket", catalog_dir=data_dir, channel="garden", active_steps={_step_uri(co2_path)}
    )

    assert f"emissions/owid_co2/{DATASET_JSONLD_FILENAME}" in captured["keys"]
    assert SITEMAP_FILENAME in captured["keys"]
    assert QUALITY_REPORT_FILENAME in captured["keys"]
    # The stable short-key path is not itself scheduled for deletion...
    assert f"emissions/owid_co2/{DATASET_JSONLD_FILENAME}" not in captured["delete_keys"]
    # ...but the old dated catalog-path location is, so it doesn't linger as duplicate content.
    assert f"{co2_path}/{DATASET_JSONLD_FILENAME}" in captured["delete_keys"]


def test_build_and_publish_catalog_jsonld_deletes_short_key_for_skipped_dataset(tmp_path: Path, monkeypatch) -> None:
    """A dataset that fails the quality gate (e.g. becomes non-redistributable) must have its
    live short-key JSON-LD scheduled for deletion — a prior publish may have emitted it, and
    an ineligible dataset must stop being served."""
    data_dir = tmp_path / "data"
    restricted_path = _add_eligible_dataset(data_dir, namespace="wb", dataset="restricted", non_redistributable=True)
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    captured: dict[str, Any] = {}

    def fake_sync_jsonld_artifacts(s3, bucket, catalog_dir, keys, delete_keys=None):
        captured["keys"] = keys
        captured["delete_keys"] = delete_keys

    monkeypatch.setattr("etl.catalog_jsonld.publish.connect_r2", lambda: object())
    monkeypatch.setattr("etl.catalog_jsonld.publish.sync_jsonld_artifacts", fake_sync_jsonld_artifacts)

    build_and_publish_catalog_jsonld(
        bucket="test-bucket", catalog_dir=data_dir, channel="garden", active_steps={_step_uri(restricted_path)}
    )

    assert f"wb/restricted/{DATASET_JSONLD_FILENAME}" not in captured["keys"]
    assert f"wb/restricted/{DATASET_JSONLD_FILENAME}" in captured["delete_keys"]
    assert f"{restricted_path}/{DATASET_JSONLD_FILENAME}" in captured["delete_keys"]


def test_build_and_publish_catalog_jsonld_deletes_both_locations_for_archived_dataset(
    tmp_path: Path, monkeypatch
) -> None:
    """A dataset removed from the DAG with no active replacement at all must have both its old
    dated-path and stable short-key JSON-LD scheduled for deletion, even though it never shows
    up as emitted or skipped (no on-disk version of it is active)."""
    data_dir = tmp_path / "data"
    archived_path = _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2", version="2024-01-01")
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    captured: dict[str, Any] = {}

    def fake_sync_jsonld_artifacts(s3, bucket, catalog_dir, keys, delete_keys=None):
        captured["keys"] = keys
        captured["delete_keys"] = delete_keys

    monkeypatch.setattr("etl.catalog_jsonld.publish.connect_r2", lambda: object())
    monkeypatch.setattr("etl.catalog_jsonld.publish.sync_jsonld_artifacts", fake_sync_jsonld_artifacts)

    build_and_publish_catalog_jsonld(bucket="test-bucket", catalog_dir=data_dir, channel="garden", active_steps=set())

    assert f"emissions/owid_co2/{DATASET_JSONLD_FILENAME}" not in captured["keys"]
    assert f"emissions/owid_co2/{DATASET_JSONLD_FILENAME}" in captured["delete_keys"]
    assert f"{archived_path}/{DATASET_JSONLD_FILENAME}" in captured["delete_keys"]


def test_build_and_publish_catalog_jsonld_deletes_dated_path_for_superseded_version(
    tmp_path: Path, monkeypatch
) -> None:
    """A dataset version superseded by an active replacement under the same short key (e.g. a
    stale ".../latest/..." build left behind after re-versioning to a dated one) must have its
    own old dated-path JSON-LD scheduled for deletion — but not the short key, which the
    active version legitimately owns and keeps serving."""
    data_dir = tmp_path / "data"
    stale_path = _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2", version="latest")
    current_path = _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2", version="2025-12-04")
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    captured: dict[str, Any] = {}

    def fake_sync_jsonld_artifacts(s3, bucket, catalog_dir, keys, delete_keys=None):
        captured["keys"] = keys
        captured["delete_keys"] = delete_keys

    monkeypatch.setattr("etl.catalog_jsonld.publish.connect_r2", lambda: object())
    monkeypatch.setattr("etl.catalog_jsonld.publish.sync_jsonld_artifacts", fake_sync_jsonld_artifacts)

    build_and_publish_catalog_jsonld(
        bucket="test-bucket", catalog_dir=data_dir, channel="garden", active_steps={_step_uri(current_path)}
    )

    assert f"emissions/owid_co2/{DATASET_JSONLD_FILENAME}" in captured["keys"]
    assert f"{stale_path}/{DATASET_JSONLD_FILENAME}" in captured["delete_keys"]
    assert f"{current_path}/{DATASET_JSONLD_FILENAME}" in captured["delete_keys"]
    # The short key is actively served by the current version, so it must never be deleted.
    assert f"emissions/owid_co2/{DATASET_JSONLD_FILENAME}" not in captured["delete_keys"]

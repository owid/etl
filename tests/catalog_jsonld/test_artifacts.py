import json
from pathlib import Path

import pandas as pd
from owid.catalog import Dataset, DatasetMeta, License, Origin, Table, VariableMeta
from owid.catalog.api.legacy import LocalCatalog
from structlog.testing import capture_logs

from etl.catalog_jsonld.artifacts import build_catalog_jsonld_artifacts


def _step_uri(catalog_path: str) -> str:
    """Convert a catalog dataset path (e.g. 'garden/ns/2025-01-01/short_name') to its DAG step URI."""
    return f"data://{catalog_path}"


def _add_eligible_dataset(
    data_dir: Path,
    *,
    namespace: str,
    dataset: str,
    version: str = "2025-01-01",
    non_redistributable: bool = False,
) -> str:
    """Create a minimal, quality-eligible garden dataset and return its catalog path."""
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


def test_build_catalog_jsonld_artifacts_writes_dataset_jsonld_sitemap_and_report(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    dataset_dir = data_dir / "garden" / "example" / "2025-01-01" / "example_dataset"
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
            namespace="example",
            version="2025-01-01",
            short_name="example_dataset",
            title="Example dataset",
            description="Dataset description",
        ),
    )
    tb = Table(pd.DataFrame({"year": [2020], "value": [1]}), short_name="example_table")
    tb = tb.set_index("year")
    tb.metadata.title = "Example table"
    tb.metadata.description = "Table description"
    tb._fields["value"] = VariableMeta(title="Value", description_short="A measured value.", origins=[origin])
    ds.add(tb)
    ds.save()

    from owid.catalog.api.legacy import LocalCatalog

    LocalCatalog(data_dir, channels=("garden",)).reindex()

    result = build_catalog_jsonld_artifacts(
        catalog_dir=data_dir,
        channel="garden",
        active_steps={_step_uri("garden/example/2025-01-01/example_dataset")},
    )

    assert result.emitted == ["garden/example/2025-01-01/example_dataset"]
    # dataset.jsonld now lives at the stable "<namespace>/<dataset>" short key, not inside the
    # dataset's own dated catalog folder.
    jsonld_path = data_dir / "example" / "example_dataset" / "dataset.jsonld"
    assert jsonld_path.exists()
    assert not (dataset_dir / "dataset.jsonld").exists()
    jsonld = json.loads(jsonld_path.read_text())
    assert jsonld["name"] == "Example dataset"
    assert jsonld["url"] == "https://catalog.ourworldindata.org/example/example_dataset/"
    assert jsonld["identifier"] == "garden/example/2025-01-01/example_dataset"
    assert jsonld["version"] == "2025-01-01"
    assert jsonld["license"] == "https://creativecommons.org/licenses/by/4.0/"
    assert jsonld["temporalCoverage"] == "2020"
    assert jsonld["variableMeasured"][0]["identifier"] == "value"
    sitemap = (data_dir / "sitemap.xml").read_text()
    assert sitemap.count("<url>") == 1
    assert "<loc>https://catalog.ourworldindata.org/example/example_dataset/</loc>" in sitemap
    assert "<lastmod>2025-01-01</lastmod>" in sitemap
    report = json.loads((data_dir / "jsonld_quality_report.json").read_text())
    assert report["summary"]["emitted"] == 1
    assert report["summary"]["skipped"] == 0


def test_build_catalog_jsonld_artifacts_cleans_up_stale_old_location_for_emitted_dataset(tmp_path: Path) -> None:
    """A dataset.jsonld left over at the old dated-path location (from before this migration)
    must be removed locally even though the dataset is still emitted, since it's superseded
    by the new short-key location."""
    data_dir = tmp_path / "data"
    dataset_dir = data_dir / "garden" / "example" / "2025-01-01" / "example_dataset"
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
            namespace="example",
            version="2025-01-01",
            short_name="example_dataset",
            title="Example dataset",
            description="Dataset description",
        ),
    )
    tb = Table(pd.DataFrame({"year": [2020], "value": [1]}), short_name="example_table")
    tb = tb.set_index("year")
    tb.metadata.title = "Example table"
    tb.metadata.description = "Table description"
    tb._fields["value"] = VariableMeta(title="Value", description_short="A measured value.", origins=[origin])
    ds.add(tb)
    ds.save()
    stale_old_location = dataset_dir / "dataset.jsonld"
    stale_old_location.write_text("{}")

    LocalCatalog(data_dir, channels=("garden",)).reindex()
    build_catalog_jsonld_artifacts(
        catalog_dir=data_dir,
        channel="garden",
        active_steps={_step_uri("garden/example/2025-01-01/example_dataset")},
    )

    assert not stale_old_location.exists()
    assert (data_dir / "example" / "example_dataset" / "dataset.jsonld").exists()


def test_build_catalog_jsonld_artifacts_only_allowlist_restricts_emission(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    co2_path = _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2")
    energy_path = _add_eligible_dataset(data_dir, namespace="energy_data", dataset="owid_energy")
    population_path = _add_eligible_dataset(data_dir, namespace="demography", dataset="population")
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    result = build_catalog_jsonld_artifacts(
        catalog_dir=data_dir,
        channel="garden",
        only={"emissions/owid_co2"},
        active_steps={_step_uri(p) for p in (co2_path, energy_path, population_path)},
    )

    # Only the allowlisted dataset is emitted; the others are not even assessed.
    assert result.emitted == [co2_path]
    assert [entry.short_key for entry in result.emitted_entries] == ["emissions/owid_co2"]
    assert (data_dir / "emissions" / "owid_co2" / "dataset.jsonld").exists()
    assert not (data_dir / "garden" / "emissions" / "2025-01-01" / "owid_co2" / "dataset.jsonld").exists()
    assert not (data_dir / "energy_data" / "owid_energy" / "dataset.jsonld").exists()
    # Sitemap lists exactly the allowlisted dataset, at its stable short-key URL.
    sitemap = (data_dir / "sitemap.xml").read_text()
    assert sitemap.count("<url>") == 1
    assert "<loc>https://catalog.ourworldindata.org/emissions/owid_co2/</loc>" in sitemap
    assert co2_path not in sitemap


def test_build_catalog_jsonld_artifacts_only_is_version_agnostic(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    # Two versions of the same dataset: the allowlist matches on namespace/dataset, latest wins.
    older = _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2", version="2024-01-01")
    newer = _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2", version="2025-12-04")
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    result = build_catalog_jsonld_artifacts(
        catalog_dir=data_dir,
        channel="garden",
        only={"emissions/owid_co2"},
        active_steps={_step_uri(older), _step_uri(newer)},
    )

    assert result.emitted == [newer]


def test_build_catalog_jsonld_artifacts_ignores_stale_archived_latest_version(tmp_path: Path) -> None:
    # Regression test: a dataset re-versioned from "latest" to a dated version leaves a stale
    # on-disk "latest" build behind. Since the string "latest" sorts after any YYYY-MM-DD
    # version, naive version-max selection would wrongly pick the stale archived build over
    # the real, active, dated one. Excluding non-active-DAG steps must prevent that.
    data_dir = tmp_path / "data"
    stale_latest = _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2", version="latest")
    current = _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2", version="2025-12-04")
    LocalCatalog(data_dir, channels=("garden",)).reindex()
    # A prior build (from before "latest" was superseded) may have left a dataset.jsonld at
    # its own dated path — it must be cleaned up even though the dataset itself is still active
    # under a different version.
    stale_dated = data_dir / stale_latest / "dataset.jsonld"
    stale_dated.write_text('{"from": "before supersession"}')

    result = build_catalog_jsonld_artifacts(
        catalog_dir=data_dir,
        channel="garden",
        # Only the dated version is in the active DAG; "latest" is a stale, archived leftover.
        active_steps={_step_uri(current)},
    )

    assert result.emitted == [current]
    assert stale_latest not in result.emitted
    # The dataset has an active replacement, so it's superseded, not archived outright.
    assert result.archived_entries == []
    assert [entry.catalog_path for entry in result.superseded_entries] == [stale_latest]
    assert not stale_dated.exists()


def test_build_catalog_jsonld_artifacts_cleans_up_dataset_archived_with_no_replacement(tmp_path: Path) -> None:
    # Regression test: unlike the "superseded" case above, a dataset removed from the DAG with
    # NO active replacement at all has every on-disk version excluded from latest_dataset_paths,
    # so it never becomes an emitted or skipped entry either. Without explicit archived-entry
    # tracking, a dataset.jsonld it published before being archived (at its old dated path and
    # its stable short key) would never be scheduled for deletion again.
    data_dir = tmp_path / "data"
    archived = _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2", version="2024-01-01")
    LocalCatalog(data_dir, channels=("garden",)).reindex()
    stale_dated = data_dir / archived / "dataset.jsonld"
    stale_dated.write_text('{"from": "before archiving"}')
    stale_short_key = data_dir / "emissions" / "owid_co2" / "dataset.jsonld"
    stale_short_key.parent.mkdir(parents=True)
    stale_short_key.write_text('{"from": "before archiving"}')

    result = build_catalog_jsonld_artifacts(
        catalog_dir=data_dir,
        channel="garden",
        # Nothing is active: the dataset has been archived with no replacement.
        active_steps=set(),
    )

    assert result.emitted == []
    assert result.skipped == []
    assert [entry.catalog_path for entry in result.archived_entries] == [archived]
    assert not stale_dated.exists()
    assert not stale_short_key.exists()


def test_build_catalog_jsonld_artifacts_archived_multi_table_dataset_yields_one_entry(tmp_path: Path) -> None:
    # Regression test: LocalCatalog.frame has one row per table, but dataset_path strips the
    # table name — a multi-table archived dataset must still yield exactly one archived_entries
    # item, not one per table (which would inflate report counts and schedule redundant
    # HEAD/DELETE calls against the same dataset.jsonld key on every publish).
    data_dir = tmp_path / "data"
    dataset_dir = data_dir / "garden" / "emissions" / "2024-01-01" / "owid_co2"
    ds = Dataset.create_empty(
        dataset_dir,
        DatasetMeta(channel="garden", namespace="emissions", version="2024-01-01", short_name="owid_co2"),
    )
    for table_name in ("table_one", "table_two"):
        tb = Table(pd.DataFrame({"year": [2020], "value": [1]}), short_name=table_name)
        tb = tb.set_index("year")
        tb.metadata.title = "Example table"
        tb.metadata.description = "Table description"
        ds.add(tb)
    ds.save()
    archived = "garden/emissions/2024-01-01/owid_co2"
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    result = build_catalog_jsonld_artifacts(catalog_dir=data_dir, channel="garden", active_steps=set())

    assert [entry.catalog_path for entry in result.archived_entries] == [archived]


def test_build_catalog_jsonld_artifacts_only_unmatched_entry_warns_and_emits_nothing(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    co2_path = _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2")
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    with capture_logs() as logs:
        result = build_catalog_jsonld_artifacts(
            catalog_dir=data_dir,
            channel="garden",
            only={"wb/does_not_exist"},
            active_steps={_step_uri(co2_path)},
        )

    assert result.emitted == []
    assert (data_dir / "sitemap.xml").read_text().count("<url>") == 0
    warnings = [entry for entry in logs if entry["event"] == "catalog_jsonld.allowlist_entry_unmatched"]
    assert [entry["dataset"] for entry in warnings] == ["wb/does_not_exist"]


def test_build_catalog_jsonld_artifacts_excludes_non_redistributable(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    restricted_path = _add_eligible_dataset(data_dir, namespace="wb", dataset="restricted", non_redistributable=True)
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    result = build_catalog_jsonld_artifacts(
        catalog_dir=data_dir, channel="garden", active_steps={_step_uri(restricted_path)}
    )

    assert result.emitted == []
    assert [item.catalog_path for item in result.skipped] == ["garden/wb/2025-01-01/restricted"]
    assert "non_redistributable" in result.skipped[0].blockers
    assert not (data_dir / "garden" / "wb" / "2025-01-01" / "restricted" / "dataset.jsonld").exists()


def test_build_catalog_jsonld_artifacts_omits_lastmod_for_non_date_version(tmp_path: Path) -> None:
    """Datasets versioned "latest" have no real modification date; the sitemap entry must omit
    lastmod rather than stamping the build date, which would falsely mark the page as modified
    on every publish."""
    data_dir = tmp_path / "data"
    path = _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2", version="latest")
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    result = build_catalog_jsonld_artifacts(catalog_dir=data_dir, channel="garden", active_steps={_step_uri(path)})

    assert result.emitted == ["garden/emissions/latest/owid_co2"]
    sitemap = (data_dir / "sitemap.xml").read_text()
    assert "<loc>https://catalog.ourworldindata.org/emissions/owid_co2/</loc>" in sitemap
    assert "<lastmod>" not in sitemap


def test_build_catalog_jsonld_artifacts_removes_short_key_artifact_when_dataset_becomes_ineligible(
    tmp_path: Path,
) -> None:
    """A dataset emitted at its short key by a prior build must have that local artifact removed
    once it fails a quality gate, so an ineligible dataset can't keep a stale public JSON-LD."""
    data_dir = tmp_path / "data"
    path = _add_eligible_dataset(data_dir, namespace="wb", dataset="restricted", non_redistributable=True)
    LocalCatalog(data_dir, channels=("garden",)).reindex()
    stale_short_key = data_dir / "wb" / "restricted" / "dataset.jsonld"
    stale_short_key.parent.mkdir(parents=True)
    stale_short_key.write_text('{"from": "a prior build"}')

    result = build_catalog_jsonld_artifacts(catalog_dir=data_dir, channel="garden", active_steps={_step_uri(path)})

    assert result.emitted == []
    assert [entry.short_key for entry in result.skipped_entries] == ["wb/restricted"]
    assert not stale_short_key.exists()


def test_build_catalog_jsonld_artifacts_blocks_reserved_namespace(tmp_path: Path) -> None:
    """A namespace that collides with a catalog channel name (e.g. "garden") would shadow a
    root-level catalog_dir entry if used as a short-key first segment, so it's blocked."""
    data_dir = tmp_path / "data"
    path = _add_eligible_dataset(data_dir, namespace="garden", dataset="something")
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    result = build_catalog_jsonld_artifacts(catalog_dir=data_dir, channel="garden", active_steps={_step_uri(path)})

    assert result.emitted == []
    assert [item.catalog_path for item in result.skipped] == [path]
    assert "reserved_namespace" in result.skipped[0].blockers
    assert not (data_dir / "garden" / "something" / "dataset.jsonld").exists()


def test_build_catalog_jsonld_artifacts_blocks_duplicate_short_keys(tmp_path: Path, monkeypatch) -> None:
    """No two emitted datasets in a build may resolve to the same "<namespace>/<dataset>" short
    key. This can't happen today (latest_dataset_paths already dedupes per (channel, namespace,
    dataset) and the builder is single-channel), so the collision is forced here to exercise the
    batch-level guard that protects a future multi-channel build."""
    data_dir = tmp_path / "data"
    path_a = _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2")
    path_b = _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2_dup")
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    import etl.catalog_jsonld.artifacts as artifacts_module

    real_latest_dataset_paths = artifacts_module.latest_dataset_paths

    def fake_latest_dataset_paths(frame, *, channel="garden", only=None, active_steps=None):
        entries = real_latest_dataset_paths(frame, channel=channel, only=only, active_steps=active_steps)
        return [
            artifacts_module.LatestDatasetPath(
                catalog_path=entry.catalog_path, namespace="emissions", dataset="owid_co2", version=entry.version
            )
            for entry in entries
        ]

    monkeypatch.setattr(artifacts_module, "latest_dataset_paths", fake_latest_dataset_paths)

    result = artifacts_module.build_catalog_jsonld_artifacts(
        catalog_dir=data_dir,
        channel="garden",
        active_steps={_step_uri(path_a), _step_uri(path_b)},
    )

    assert result.emitted == []
    assert {item.catalog_path for item in result.skipped} == {path_a, path_b}
    assert all("duplicate_short_key" in item.blockers for item in result.skipped)

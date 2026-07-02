import json
from pathlib import Path

import pandas as pd
from owid.catalog import Dataset, DatasetMeta, License, Origin, Table, VariableMeta
from owid.catalog.api.legacy import LocalCatalog
from structlog.testing import capture_logs

from etl.catalog_jsonld.artifacts import build_catalog_jsonld_artifacts


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

    result = build_catalog_jsonld_artifacts(catalog_dir=data_dir, channel="garden")

    assert result.emitted == ["garden/example/2025-01-01/example_dataset"]
    jsonld_path = dataset_dir / "dataset.jsonld"
    assert jsonld_path.exists()
    jsonld = json.loads(jsonld_path.read_text())
    assert jsonld["name"] == "Example dataset"
    assert jsonld["license"] == "https://creativecommons.org/licenses/by/4.0/"
    assert jsonld["temporalCoverage"] == "2020"
    assert jsonld["variableMeasured"][0]["identifier"] == "value"
    assert (data_dir / "sitemap.xml").read_text().count("<url>") == 1
    report = json.loads((data_dir / "jsonld_quality_report.json").read_text())
    assert report["summary"]["emitted"] == 1
    assert report["summary"]["skipped"] == 0


def test_build_catalog_jsonld_artifacts_only_allowlist_restricts_emission(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    co2_path = _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2")
    _add_eligible_dataset(data_dir, namespace="energy_data", dataset="owid_energy")
    _add_eligible_dataset(data_dir, namespace="demography", dataset="population")
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    result = build_catalog_jsonld_artifacts(catalog_dir=data_dir, channel="garden", only={"emissions/owid_co2"})

    # Only the allowlisted dataset is emitted; the others are not even assessed.
    assert result.emitted == [co2_path]
    assert (data_dir / "garden" / "emissions" / "2025-01-01" / "owid_co2" / "dataset.jsonld").exists()
    assert not (data_dir / "garden" / "energy_data" / "2025-01-01" / "owid_energy" / "dataset.jsonld").exists()
    # Sitemap lists exactly the allowlisted dataset.
    sitemap = (data_dir / "sitemap.xml").read_text()
    assert sitemap.count("<url>") == 1
    assert co2_path in sitemap


def test_build_catalog_jsonld_artifacts_only_is_version_agnostic(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    # Two versions of the same dataset: the allowlist matches on namespace/dataset, latest wins.
    _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2", version="2024-01-01")
    newer = _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2", version="2025-12-04")
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    result = build_catalog_jsonld_artifacts(catalog_dir=data_dir, channel="garden", only={"emissions/owid_co2"})

    assert result.emitted == [newer]


def test_build_catalog_jsonld_artifacts_only_unmatched_entry_warns_and_emits_nothing(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _add_eligible_dataset(data_dir, namespace="emissions", dataset="owid_co2")
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    with capture_logs() as logs:
        result = build_catalog_jsonld_artifacts(catalog_dir=data_dir, channel="garden", only={"wb/does_not_exist"})

    assert result.emitted == []
    assert (data_dir / "sitemap.xml").read_text().count("<url>") == 0
    warnings = [entry for entry in logs if entry["event"] == "catalog_jsonld.allowlist_entry_unmatched"]
    assert [entry["dataset"] for entry in warnings] == ["wb/does_not_exist"]


def test_build_catalog_jsonld_artifacts_excludes_non_redistributable(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _add_eligible_dataset(data_dir, namespace="wb", dataset="restricted", non_redistributable=True)
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    result = build_catalog_jsonld_artifacts(catalog_dir=data_dir, channel="garden")

    assert result.emitted == []
    assert [item.catalog_path for item in result.skipped] == ["garden/wb/2025-01-01/restricted"]
    assert "non_redistributable" in result.skipped[0].blockers
    assert not (data_dir / "garden" / "wb" / "2025-01-01" / "restricted" / "dataset.jsonld").exists()

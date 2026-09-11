import json
from pathlib import Path

import pandas as pd
from owid.catalog import Dataset, DatasetMeta, License, Origin, Table, VariableMeta
from owid.catalog.api.legacy import LocalCatalog

from etl.catalog_pages.artifacts import build_catalog_page_artifacts
from etl.catalog_pages.pages import MANIFEST_VERSION, page_filenames

ORIGIN = Origin(
    producer="Example Producer",
    title="Original dataset",
    description="Original description",
    date_published="2025-01-01",
    url_main="https://example.org/data",
    citation_full="Example Producer (2025). Original dataset.",
    license=License(name="CC BY 4.0", url="https://example.org/license"),
)


def _step_uri(catalog_path: str) -> str:
    return f"data://{catalog_path}"


def _add_dataset(
    data_dir: Path,
    *,
    namespace: str = "energy",
    dataset: str = "owid_energy",
    version: str = "2026-09-10",
    description: str | None = "Dataset description.\n\n## Changelog\n\n- 2026-09-10: First release.",
    tables: int = 1,
) -> str:
    dataset_dir = data_dir / "garden" / namespace / version / dataset
    ds = Dataset.create_empty(
        dataset_dir,
        DatasetMeta(
            channel="garden",
            namespace=namespace,
            version=version,
            short_name=dataset,
            title="Energy dataset",
            description=description,
            licenses=[License(name="CC BY 4.0", url="https://creativecommons.org/licenses/by/4.0/")],
            jsonld=True,
        ),
    )
    for index in range(tables):
        name = dataset if index == 0 else f"extra_{index}"
        tb = Table(
            pd.DataFrame({"country": ["Spain", "France"], "year": [2020, 2020], "hydro_energy_twh": [1.5, 2.5]}),
            short_name=name,
        )
        tb = tb.set_index(["country", "year"])
        tb.metadata.title = f"{name} table"
        tb._fields["hydro_energy_twh"] = VariableMeta(
            title="Hydropower", description_short="Energy from hydropower.", unit="terawatt-hours", origins=[ORIGIN]
        )
        ds.add(tb)
    ds.save()
    LocalCatalog(data_dir, channels=("garden",)).reindex()
    return f"garden/{namespace}/{version}/{dataset}"


def test_build_writes_page_files_and_manifest(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    catalog_path = _add_dataset(data_dir)

    result = build_catalog_page_artifacts(
        catalog_dir=data_dir, channel="garden", active_steps={_step_uri(catalog_path)}
    )

    assert result.pages == [catalog_path]
    assert result.emitted == [catalog_path]
    page_dir = data_dir / "energy" / "owid_energy"
    assert sorted(path.name for path in page_dir.iterdir()) == [
        "codebook.csv",
        "dataset.jsonld",
        "manifest.json",
        "owid_energy.csv",
        "owid_energy.xlsx",
        "readme.md",
        "sources.csv",
    ]
    assert sorted(result.page_keys) == sorted(
        f"energy/owid_energy/{name}" for name in page_filenames(Dataset(data_dir / catalog_path))
    )

    data = pd.read_csv(page_dir / "owid_energy.csv")
    assert data.columns.tolist() == ["country", "year", "hydro_energy_twh"]
    assert len(data) == 2
    codebook = pd.read_csv(page_dir / "codebook.csv")
    assert codebook["column"].tolist() == ["country", "year", "hydro_energy_twh"]
    assert (
        codebook.set_index("column").loc["hydro_energy_twh", "source"] == "Example Producer – Original dataset (2025)"
    )
    readme = (page_dir / "readme.md").read_text()
    assert readme.startswith("# Energy dataset\n")
    assert "https://catalog.ourworldindata.org/energy/owid_energy/" in readme
    assert "## Changelog" in readme
    assert "### Example Producer – Original dataset (2025)" in readme

    manifest = json.loads((page_dir / "manifest.json").read_text())
    assert manifest["manifest_version"] == MANIFEST_VERSION
    assert manifest["catalog_path"] == catalog_path
    assert manifest["namespace"] == "energy"
    assert manifest["short_name"] == "owid_energy"
    assert manifest["version"] == "2026-09-10"
    assert manifest["title"] == "Energy dataset"
    assert manifest["description"].startswith("Dataset description.")
    assert manifest["published_at"].endswith("Z")
    assert manifest["license"] == {"name": "CC BY 4.0", "url": "https://creativecommons.org/licenses/by/4.0/"}
    assert manifest["readme"] == "readme.md"
    assert manifest["codebook"] == "codebook.csv"
    assert manifest["sources"] == "sources.csv"
    sources = pd.read_csv(page_dir / "sources.csv")
    assert sources["label"].tolist() == ["Example Producer – Original dataset (2025)"]
    assert manifest["jsonld"] == "dataset.jsonld"
    assert manifest["tables"] == [
        {"name": "owid_energy", "title": "owid_energy table", "description": None, "rows": 2, "columns": 3}
    ]
    files = {entry["name"]: entry for entry in manifest["files"]}
    assert set(files) == {
        "owid_energy.csv",
        "owid_energy.xlsx",
        "codebook.csv",
        "sources.csv",
        "readme.md",
        "owid_energy.feather",
    }
    csv = files["owid_energy.csv"]
    assert csv["format"] == "csv"
    assert csv["table"] == "owid_energy"
    assert csv["url"] == "https://catalog.ourworldindata.org/energy/owid_energy/owid_energy.csv"
    assert csv["size_bytes"] == (page_dir / "owid_energy.csv").stat().st_size > 0
    feather = files["owid_energy.feather"]
    assert feather["versioned"] is True
    assert feather["url"] == f"https://catalog.ourworldindata.org/{catalog_path}/owid_energy.feather"
    assert all(entry["url"].startswith("https://") for entry in manifest["files"])

    sitemap = (data_dir / "sitemap.xml").read_text()
    assert "<loc>https://catalog.ourworldindata.org/energy/owid_energy/</loc>" in sitemap
    report = json.loads((data_dir / "jsonld_quality_report.json").read_text())
    assert report["summary"]["pages"] == 1


def test_build_writes_one_csv_per_table(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    catalog_path = _add_dataset(data_dir, tables=2)

    build_catalog_page_artifacts(catalog_dir=data_dir, channel="garden", active_steps={_step_uri(catalog_path)})

    page_dir = data_dir / "energy" / "owid_energy"
    assert (page_dir / "owid_energy.csv").exists()
    assert (page_dir / "extra_1.csv").exists()
    manifest = json.loads((page_dir / "manifest.json").read_text())
    # The main table (named after the dataset) comes first.
    assert [table["name"] for table in manifest["tables"]] == ["owid_energy", "extra_1"]
    codebook = pd.read_csv(page_dir / "codebook.csv")
    assert codebook.columns.tolist()[:2] == ["table", "column"]


def test_page_is_written_even_when_jsonld_gates_fail(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    # No dataset description: a JSON-LD blocker ("missing_description"), but not a page blocker.
    catalog_path = _add_dataset(data_dir, description=None)

    result = build_catalog_page_artifacts(
        catalog_dir=data_dir, channel="garden", active_steps={_step_uri(catalog_path)}
    )

    assert result.pages == [catalog_path]
    assert result.emitted == []
    assert [item.catalog_path for item in result.skipped] == [catalog_path]
    page_dir = data_dir / "energy" / "owid_energy"
    assert (page_dir / "manifest.json").exists()
    assert not (page_dir / "dataset.jsonld").exists()
    manifest = json.loads((page_dir / "manifest.json").read_text())
    assert "jsonld" not in manifest
    assert "energy/owid_energy/dataset.jsonld" not in result.page_keys


def test_non_redistributable_dataset_gets_no_page_and_stale_files_are_removed(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    catalog_path = _add_dataset(data_dir)
    build_catalog_page_artifacts(catalog_dir=data_dir, channel="garden", active_steps={_step_uri(catalog_path)})
    page_dir = data_dir / "energy" / "owid_energy"
    assert (page_dir / "manifest.json").exists()

    ds = Dataset(data_dir / catalog_path)
    ds.metadata.non_redistributable = True
    ds.save()
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    result = build_catalog_page_artifacts(
        catalog_dir=data_dir, channel="garden", active_steps={_step_uri(catalog_path)}
    )

    assert result.pages == []
    assert [item.catalog_path for item in result.skipped] == [catalog_path]
    assert not any(page_dir.iterdir())

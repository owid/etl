import json
from pathlib import Path

import pandas as pd
from owid.catalog import Dataset, DatasetMeta, License, Origin, Table, VariableMeta, VariablePresentationMeta
from owid.catalog.api.legacy import LocalCatalog

from etl.catalog_pages.artifacts import build_catalog_page_artifacts
from etl.catalog_pages.pages import MANIFEST_VERSION, page_filenames
from etl.catalog_pages.publish import _page_keys

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
            owners=["Pablo Rosado", "Mojmír Vinkler"],
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
            title="Hydropower",
            description_short="Energy from hydropower.",
            unit="terawatt-hours",
            origins=[ORIGIN],
            presentation=VariablePresentationMeta(topic_tags=["Energy"]),
        )
        ds.add(tb)
    ds.save()
    LocalCatalog(data_dir, channels=("garden",)).reindex()
    return f"garden/{namespace}/{version}/{dataset}"


def test_build_writes_page_files_and_manifest(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    catalog_path = _add_dataset(data_dir)

    result = build_catalog_page_artifacts(
        output_dir=data_dir, catalog_dir=data_dir, channel="garden", active_steps={_step_uri(catalog_path)}
    )

    assert result.pages == [catalog_path]
    assert result.emitted == [catalog_path]
    page_dir = data_dir / "energy" / "owid_energy"
    assert sorted(path.name for path in page_dir.iterdir()) == [
        "dataset.jsonld",
        "manifest.json",
        "owid_energy.codebook.csv",
        "owid_energy.csv",
        "owid_energy.parquet",
        "owid_energy.sources.csv",
        "owid_energy.xlsx",
        "readme.md",
    ]
    assert sorted(key for key in result.page_keys if key.startswith("energy/")) == sorted(
        f"energy/owid_energy/{name}" for name in page_filenames(Dataset(data_dir / catalog_path))
    )

    data = pd.read_csv(page_dir / "owid_energy.csv")
    assert data.columns.tolist() == ["country", "year", "hydro_energy_twh"]
    assert len(data) == 2
    codebook = pd.read_csv(page_dir / "owid_energy.codebook.csv")
    assert codebook["column"].tolist() == ["country", "year", "hydro_energy_twh"]
    assert (
        codebook.set_index("column").loc["hydro_energy_twh", "source"] == "Example Producer – Original dataset (2025)"
    )
    readme = (page_dir / "readme.md").read_text()
    assert readme.startswith("# Energy dataset\n\n## About this dataset\n")
    assert "- Catalog page:\n  - https://catalog.ourworldindata.org/energy/owid_energy/" in readme
    assert "## Changelog" in readme
    assert "##### Example Producer – Original dataset (2025)" in readme

    manifest = json.loads((page_dir / "manifest.json").read_text())
    assert manifest["manifest_version"] == MANIFEST_VERSION
    assert manifest["catalog_path"] == catalog_path
    assert manifest["namespace"] == "energy"
    assert manifest["short_name"] == "owid_energy"
    assert manifest["version"] == "2026-09-10"
    assert manifest["title"] == "Energy dataset"
    assert manifest["description"].startswith("Dataset description.")
    assert manifest["published_at"].endswith("Z")
    # No license statement anywhere: OWID republishes data produced by others.
    assert "license" not in manifest
    assert manifest["readme"] == "readme.md"
    assert "codebook" not in manifest and "sources" not in manifest
    sources = pd.read_csv(page_dir / "owid_energy.sources.csv")
    assert sources["label"].tolist() == ["Example Producer – Original dataset (2025)"]
    workbook = pd.ExcelFile(page_dir / "owid_energy.xlsx")
    assert workbook.sheet_names == ["data", "codebook", "sources"]
    assert pd.read_excel(workbook, "data").columns.tolist() == ["country", "year", "hydro_energy_twh"]
    assert manifest["jsonld"] == "dataset.jsonld"
    # Owners in order, first the accountable one; a name without a team page carries no url.
    assert manifest["owners"] == [
        {"name": "Pablo Rosado", "url": "https://ourworldindata.org/team/pablo-rosado"},
        {"name": "Mojmír Vinkler"},
    ]
    # The primary topic gives the page its way back to the charts.
    assert manifest["topics"] == ["Energy"]
    assert manifest["explore_url"] == "https://ourworldindata.org/search?q=Energy&resultType=all"
    assert manifest["tables"] == [
        {
            "name": "owid_energy",
            "title": "owid_energy table",
            "description": None,
            "rows": 2,
            "columns": 3,
            "codebook": "owid_energy.codebook.csv",
            "sources": "owid_energy.sources.csv",
            "citation": "Example Producer (2025) – with minor processing by Our World in Data",
        }
    ]
    files = {entry["name"]: entry for entry in manifest["files"] if not entry.get("versioned")}
    dated = {entry["name"]: entry for entry in manifest["files"] if entry.get("versioned")}
    assert {name: entry["role"] for name, entry in files.items()} == {
        "owid_energy.csv": "data",
        "owid_energy.parquet": "data",
        "owid_energy.xlsx": "data",
        "owid_energy.codebook.csv": "documentation",
        "owid_energy.sources.csv": "documentation",
        "readme.md": "documentation",
    }
    # The links to this version are the pipeline's own dated files; the page writes nothing into that folder,
    # which belongs to the pipeline, and uploads nothing there.
    assert {name: entry["role"] for name, entry in dated.items()} == {
        "owid_energy.feather": "archive",
        "owid_energy.meta.json": "archive",
    }
    assert not list((data_dir / catalog_path).glob("*.csv")) and not list((data_dir / catalog_path).glob("*.xlsx"))
    assert not [key for key in result.page_keys if key.startswith(f"{catalog_path}/")]
    assert files["owid_energy.codebook.csv"]["table"] == "owid_energy"
    assert "table" not in files["readme.md"]
    csv = files["owid_energy.csv"]
    assert csv["format"] == "csv"
    assert csv["table"] == "owid_energy"
    assert csv["url"] == "https://catalog.ourworldindata.org/energy/owid_energy/owid_energy.csv"
    assert csv["size_bytes"] == (page_dir / "owid_energy.csv").stat().st_size > 0
    parquet = files["owid_energy.parquet"]
    assert parquet["table"] == "owid_energy"
    assert pd.read_parquet(page_dir / "owid_energy.parquet").columns.tolist() == ["country", "year", "hydro_energy_twh"]
    feather = dated["owid_energy.feather"]
    assert feather["versioned"] is True
    meta_json = dated["owid_energy.meta.json"]
    assert meta_json["versioned"] is True and meta_json["format"] == "json"
    assert meta_json["url"] == f"https://catalog.ourworldindata.org/{catalog_path}/owid_energy.meta.json"
    assert feather["url"] == f"https://catalog.ourworldindata.org/{catalog_path}/owid_energy.feather"
    assert all(entry["url"].startswith("https://") for entry in manifest["files"])

    # The JSON-LD lists the same data files as the manifest, so search engines see the downloads the page has.
    jsonld = json.loads((page_dir / "dataset.jsonld").read_text())
    # The dated metadata file is not a download of the data, so it stays out of the JSON-LD.
    assert [entry["name"] for entry in jsonld["distribution"]] == [
        "owid_energy.csv",
        "owid_energy.parquet",
        "owid_energy.xlsx",
        "owid_energy.feather",
    ]
    assert jsonld["distribution"][0] == {
        "@type": "DataDownload",
        "name": "owid_energy.csv",
        "encodingFormat": "text/csv",
        "contentUrl": "https://catalog.ourworldindata.org/energy/owid_energy/owid_energy.csv",
        "contentSize": str(csv["size_bytes"]),
    }
    assert jsonld["variableMeasured"][-1]["identifier"] == "hydro_energy_twh"

    sitemap = (data_dir / "sitemap.xml").read_text()
    assert "<loc>https://catalog.ourworldindata.org/energy/owid_energy/</loc>" in sitemap
    report = json.loads((data_dir / "jsonld_quality_report.json").read_text())
    assert report["summary"]["pages"] == 1


def test_build_writes_one_csv_per_table(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    catalog_path = _add_dataset(data_dir, tables=2)

    build_catalog_page_artifacts(
        output_dir=data_dir, catalog_dir=data_dir, channel="garden", active_steps={_step_uri(catalog_path)}
    )

    page_dir = data_dir / "energy" / "owid_energy"
    assert (page_dir / "owid_energy.csv").exists()
    assert (page_dir / "extra_1.csv").exists()
    assert (page_dir / "extra_1.parquet").exists()
    manifest = json.loads((page_dir / "manifest.json").read_text())
    # The main table (named after the dataset) comes first.
    assert [table["name"] for table in manifest["tables"]] == ["owid_energy", "extra_1"]
    # A multi-table dataset still has the variables of its main table and every table's files at the top level
    # of its JSON-LD, which is what Dataset Search reads.
    jsonld = json.loads((page_dir / "dataset.jsonld").read_text())
    assert [variable["identifier"] for variable in jsonld["variableMeasured"]] == ["hydro_energy_twh"]
    assert {entry["name"] for entry in jsonld["distribution"]} >= {"owid_energy.csv", "extra_1.csv", "extra_1.parquet"}
    assert [part["identifier"] for part in jsonld["hasPart"]] == ["owid_energy", "extra_1"]
    # Each table has its own codebook and sources; there is no dataset-wide codebook.
    assert not (page_dir / "codebook.csv").exists()
    for name in ["owid_energy", "extra_1"]:
        codebook = pd.read_csv(page_dir / f"{name}.codebook.csv")
        assert codebook.columns.tolist()[0] == "column"
        assert (page_dir / f"{name}.sources.csv").exists()
        assert (page_dir / f"{name}.xlsx").exists()
    assert [table["codebook"] for table in manifest["tables"]] == ["owid_energy.codebook.csv", "extra_1.codebook.csv"]


def test_page_is_written_even_when_jsonld_gates_fail(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    # No dataset description: a JSON-LD blocker ("missing_description"), but not a page blocker.
    catalog_path = _add_dataset(data_dir, description=None)

    result = build_catalog_page_artifacts(
        output_dir=data_dir, catalog_dir=data_dir, channel="garden", active_steps={_step_uri(catalog_path)}
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


def test_dataset_saved_as_csv_gets_a_dated_csv_link(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    catalog_path = _add_dataset(data_dir)
    ds = Dataset(data_dir / catalog_path)
    ds.add(ds["owid_energy"], formats=["feather", "csv"])
    ds.save()
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    result = build_catalog_page_artifacts(
        output_dir=data_dir, catalog_dir=data_dir, channel="garden", active_steps={_step_uri(catalog_path)}
    )

    page_dir = data_dir / "energy" / "owid_energy"
    manifest = json.loads((page_dir / "manifest.json").read_text())
    dated = {entry["name"]: entry for entry in manifest["files"] if entry.get("versioned")}
    assert set(dated) == {"owid_energy.csv", "owid_energy.feather", "owid_energy.meta.json"}
    assert dated["owid_energy.csv"]["role"] == "archive" and dated["owid_energy.csv"]["format"] == "csv"
    assert f"https://catalog.ourworldindata.org/{catalog_path}/owid_energy.csv" in (page_dir / "readme.md").read_text()
    # The pipeline's CSV is uploaded with the dataset, not by the page.
    assert not [key for key in result.page_keys if key.startswith(f"{catalog_path}/")]


def test_table_too_long_for_excel_gets_no_workbook_and_an_older_one_is_removed(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    catalog_path = _add_dataset(data_dir)
    # Two rows plus the header fit exactly.
    monkeypatch.setattr("owid.catalog.core.docs.EXCEL_MAX_ROWS", 3)
    build_catalog_page_artifacts(
        output_dir=tmp_path / "first", catalog_dir=data_dir, channel="garden", active_steps={_step_uri(catalog_path)}
    )
    assert (tmp_path / "first" / "energy" / "owid_energy" / "owid_energy.xlsx").exists()

    # The header takes one row, so two data rows no longer fit; the workbook of the earlier build must go.
    monkeypatch.setattr("owid.catalog.core.docs.EXCEL_MAX_ROWS", 2)
    result = build_catalog_page_artifacts(
        output_dir=tmp_path / "second", catalog_dir=data_dir, channel="garden", active_steps={_step_uri(catalog_path)}
    )
    page_dir = tmp_path / "second" / "energy" / "owid_energy"
    assert not (page_dir / "owid_energy.xlsx").exists()
    # The published workbook of the earlier build is deleted from R2.
    assert "energy/owid_energy/owid_energy.xlsx" in _page_keys(result, data_dir)[1]
    manifest = json.loads((page_dir / "manifest.json").read_text())
    assert manifest["tables"][0]["xlsx_skipped"] == "2 rows plus the header exceed Excel's limit of 2"
    assert not [entry for entry in manifest["files"] if entry["format"] == "xlsx"]
    # The README offers no workbook link either.
    assert ".xlsx" not in (page_dir / "readme.md").read_text()


def test_non_redistributable_dataset_gets_no_page_and_its_published_files_are_deleted(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    catalog_path = _add_dataset(data_dir)

    ds = Dataset(data_dir / catalog_path)
    ds.metadata.non_redistributable = True
    ds.save()
    LocalCatalog(data_dir, channels=("garden",)).reindex()

    result = build_catalog_page_artifacts(
        output_dir=tmp_path / "pages", catalog_dir=data_dir, channel="garden", active_steps={_step_uri(catalog_path)}
    )

    assert result.pages == []
    assert [item.catalog_path for item in result.skipped] == [catalog_path]
    assert not (tmp_path / "pages" / "energy").exists()
    delete_keys = _page_keys(result, data_dir)[1]
    assert {"energy/owid_energy/manifest.json", "energy/owid_energy/owid_energy.csv"} <= set(delete_keys)

import json
from pathlib import Path

import pandas as pd
from owid.catalog import Dataset, DatasetMeta, License, Origin, Table, VariableMeta

from etl.catalog_jsonld.artifacts import build_catalog_jsonld_artifacts


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

from owid.catalog.core.meta import DatasetMeta, License, Origin, TableMeta, VariableMeta, VariablePresentationMeta
from owid.catalog.schema_org import TableSchemaInput, dataset_to_schema_org, license_to_url


def test_single_table_dataset_flattens_table_metadata() -> None:
    origin = Origin(
        producer="Example Producer",
        title="Original dataset",
        description="Original description",
        citation_full="Example Producer (2025). Original dataset.",
        url_main="https://example.com/source",
        date_published="2025",
        license=License(name="CC BY 4.0", url="https://creativecommons.org/licenses/by/4.0/"),
    )
    variable = VariableMeta(
        title="Full flowering date",
        description_short="Day of the year with peak blossom.",
        unit="day of year",
        origins=[origin],
        presentation=VariablePresentationMeta(topic_tags=["Biodiversity"]),
    )
    table = TableSchemaInput(
        short_name="cherry_blossom",
        metadata=TableMeta(short_name="cherry_blossom", title="Cherry blossom", description="Table description"),
        variables={"year": VariableMeta(), "full_flowering_date": variable},
        formats=["feather"],
        primary_key=["year"],
        temporal_coverage="1812/2023",
        spatial_coverage="Worldwide",
    )

    jsonld = dataset_to_schema_org(
        dataset_path="garden/biodiversity/2025-04-07/cherry_blossom",
        dataset_meta=DatasetMeta(
            namespace="biodiversity",
            version="2025-04-07",
            short_name="cherry_blossom",
            title="Cherry Blossom Full Bloom Dates in Kyoto, Japan",
        ),
        tables=[table],
    )

    assert jsonld["@type"] == "Dataset"
    assert jsonld["name"] == "Cherry Blossom Full Bloom Dates in Kyoto, Japan"
    assert jsonld["license"] == "https://creativecommons.org/licenses/by/4.0/"
    assert jsonld["dateModified"] == "2025-04-07"
    assert jsonld["thumbnailUrl"] == "https://ourworldindata.org/owid-logo.svg"
    assert jsonld["publisher"]["logo"] == "https://ourworldindata.org/owid-logo.svg"
    assert jsonld["temporalCoverage"] == "1812/2023"
    assert jsonld["spatialCoverage"] == "Worldwide"
    assert jsonld["creator"] == {"@type": "Organization", "name": "Example Producer"}
    assert jsonld["isBasedOn"]["url"] == "https://example.com/source"
    assert jsonld["keywords"] == ["Biodiversity"]
    assert jsonld["variableMeasured"][0]["identifier"] == "full_flowering_date"
    assert len(jsonld["distribution"]) == 1
    assert jsonld["distribution"][0]["contentUrl"].endswith("/cherry_blossom.feather")
    assert "hasPart" not in jsonld


def test_multi_table_dataset_uses_has_part() -> None:
    origin = Origin(
        producer="Example Producer",
        title="Original dataset",
        license=License(url="https://creativecommons.org/licenses/by/4.0/"),
    )
    tables = [
        TableSchemaInput(
            short_name="table_a",
            metadata=TableMeta(short_name="table_a", title="Table A"),
            variables={"value": VariableMeta(title="Value", origins=[origin])},
            formats=["feather"],
        ),
        TableSchemaInput(
            short_name="table_b",
            metadata=TableMeta(short_name="table_b", title="Table B"),
            variables={"value": VariableMeta(title="Value", origins=[origin])},
            formats=["feather"],
        ),
    ]

    jsonld = dataset_to_schema_org(
        dataset_path="garden/example/2025-01-01/example_dataset",
        dataset_meta=DatasetMeta(
            namespace="example",
            version="2025-01-01",
            short_name="example_dataset",
            title="Example dataset",
            description="Dataset description",
        ),
        tables=tables,
    )

    assert "variableMeasured" not in jsonld
    assert [part["identifier"] for part in jsonld["hasPart"]] == ["table_a", "table_b"]
    assert jsonld["hasPart"][0]["distribution"][0]["contentUrl"].endswith("/table_a.feather")


def test_license_to_url_resolves_known_license_names() -> None:
    assert license_to_url(License(name="CC BY 4.0")) == "https://creativecommons.org/licenses/by/4.0/"
    assert license_to_url(License(name=" CC-BY 4.0 ")) == "https://creativecommons.org/licenses/by/4.0/"
    assert license_to_url(License(name="Custom license")) is None

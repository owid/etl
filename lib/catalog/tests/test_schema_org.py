from owid.catalog.core.meta import DatasetMeta, License, Origin, TableMeta, VariableMeta, VariablePresentationMeta
from owid.catalog.schema_org import TableSchemaInput, dataset_to_schema_org, license_to_url, table_description


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
        page_path="biodiversity/cherry_blossom",
        dataset_meta=DatasetMeta(
            namespace="biodiversity",
            version="2025-04-07",
            short_name="cherry_blossom",
            title="Cherry Blossom Full Bloom Dates in Kyoto, Japan",
            description="Cherry blossom bloom dates in Kyoto.",
        ),
        tables=[table],
    )

    assert jsonld["@type"] == "Dataset"
    assert jsonld["name"] == "Cherry Blossom Full Bloom Dates in Kyoto, Japan"
    # The public URL/@id are built from the stable short page_path, not the dated catalog path.
    assert jsonld["url"] == "https://catalog.ourworldindata.org/biodiversity/cherry_blossom/"
    assert jsonld["@id"] == "https://catalog.ourworldindata.org/biodiversity/cherry_blossom/#dataset"
    # identifier documents the real, dated catalog location.
    assert jsonld["identifier"] == "garden/biodiversity/2025-04-07/cherry_blossom"
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
    # Distribution content URLs still point at the real, dated file location on R2 — not the
    # short page_path used for url/@id.
    assert (
        jsonld["distribution"][0]["contentUrl"]
        == "https://catalog.ourworldindata.org/garden/biodiversity/2025-04-07/cherry_blossom/cherry_blossom.feather"
    )
    assert "hasPart" not in jsonld


def test_version_param_overrides_latest_dataset_meta_version() -> None:
    """A dataset whose own metadata version is a "latest" alias should still report a real date,
    sourced from the explicit ``version`` param (e.g. derived from the dated catalog path)."""
    table = TableSchemaInput(
        short_name="owid_co2",
        metadata=TableMeta(short_name="owid_co2", title="CO2 emissions", description="Table description"),
        variables={
            "value": VariableMeta(title="Value", origins=[Origin(producer="Example Producer", title="Origin title")])
        },
        formats=["csv"],
    )

    jsonld = dataset_to_schema_org(
        dataset_path="garden/emissions/2025-12-04/owid_co2",
        page_path="emissions/owid_co2",
        version="2025-12-04",
        dataset_meta=DatasetMeta(
            namespace="emissions",
            version="latest",
            short_name="owid_co2",
            title="CO2 emissions",
            description="CO2 emissions dataset.",
        ),
        tables=[table],
    )

    assert jsonld["version"] == "2025-12-04"
    assert jsonld["dateModified"] == "2025-12-04"


def test_version_falls_back_to_dataset_meta_version_when_not_given() -> None:
    table = TableSchemaInput(
        short_name="cherry_blossom",
        metadata=TableMeta(short_name="cherry_blossom", title="Cherry blossom", description="Table description"),
        variables={
            "value": VariableMeta(title="Value", origins=[Origin(producer="Example Producer", title="Origin title")])
        },
        formats=["csv"],
    )

    jsonld = dataset_to_schema_org(
        dataset_path="garden/biodiversity/2025-04-07/cherry_blossom",
        page_path="biodiversity/cherry_blossom",
        dataset_meta=DatasetMeta(
            namespace="biodiversity",
            version="2025-04-07",
            short_name="cherry_blossom",
            title="Cherry blossom",
            description="Cherry blossom bloom dates in Kyoto.",
        ),
        tables=[table],
    )

    assert jsonld["version"] == "2025-04-07"


def test_distribution_content_urls_use_dated_path_not_short_page_path() -> None:
    """Regression guard: data files stay at their real, dated catalog location even though the
    dataset's own url/@id move to the stable short page_path. Old dated files persist on R2
    after a new version ships, so pointing distributions there is safe and correct."""
    table = TableSchemaInput(
        short_name="owid_co2",
        metadata=TableMeta(short_name="owid_co2", title="CO2 emissions"),
        variables={"value": VariableMeta(title="Value", origins=[Origin(producer="Example Producer", title="t")])},
        formats=["feather", "csv"],
    )

    jsonld = dataset_to_schema_org(
        dataset_path="garden/emissions/2025-12-04/owid_co2",
        page_path="emissions/owid_co2",
        dataset_meta=DatasetMeta(
            namespace="emissions", version="2025-12-04", short_name="owid_co2", description="CO2 emissions dataset."
        ),
        tables=[table],
    )

    content_urls = {d["contentUrl"] for d in jsonld["distribution"]}
    assert content_urls == {
        "https://catalog.ourworldindata.org/garden/emissions/2025-12-04/owid_co2/owid_co2.feather",
        "https://catalog.ourworldindata.org/garden/emissions/2025-12-04/owid_co2/owid_co2.csv",
    }
    assert jsonld["url"] == "https://catalog.ourworldindata.org/emissions/owid_co2/"


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
        page_path="example/example_dataset",
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
    # @id uses the short page_path...
    assert jsonld["hasPart"][0]["@id"] == "https://catalog.ourworldindata.org/example/example_dataset/#table-table_a"
    # ...but the distribution's contentUrl uses the real, dated catalog path.
    assert (
        jsonld["hasPart"][0]["distribution"][0]["contentUrl"]
        == "https://catalog.ourworldindata.org/garden/example/2025-01-01/example_dataset/table_a.feather"
    )
    # Tables set no description of their own and the origin has none either, so the hasPart
    # nodes fall back to the dataset-level description rather than being left blank.
    assert jsonld["hasPart"][0]["description"] == "Dataset description"
    assert jsonld["hasPart"][1]["description"] == "Dataset description"


def test_table_description_falls_back_to_origin_then_dataset() -> None:
    origin_with_desc = Origin(producer="Producer", title="Original", description="Producer description of the data.")
    origin_without_desc = Origin(producer="Producer", title="Original")
    tables = [
        TableSchemaInput(
            short_name="with_origin_desc",
            metadata=TableMeta(short_name="with_origin_desc", title="A"),
            variables={"value": VariableMeta(title="Value", origins=[origin_with_desc])},
            formats=["feather"],
        ),
        TableSchemaInput(
            short_name="no_origin_desc",
            metadata=TableMeta(short_name="no_origin_desc", title="B"),
            variables={"value": VariableMeta(title="Value", origins=[origin_without_desc])},
            formats=["feather"],
        ),
    ]

    jsonld = dataset_to_schema_org(
        dataset_path="garden/example/2025-01-01/example_dataset",
        page_path="example/example_dataset",
        dataset_meta=DatasetMeta(
            namespace="example",
            version="2025-01-01",
            short_name="example_dataset",
            title="Example dataset",
            description="Dataset-level description",
        ),
        tables=tables,
    )

    parts = {part["identifier"]: part for part in jsonld["hasPart"]}
    # Producer (origin) description is preferred when present...
    assert parts["with_origin_desc"]["description"] == "Producer description of the data."
    # ...otherwise it falls back to the dataset description (never left blank).
    assert parts["no_origin_desc"]["description"] == "Dataset-level description"


def test_table_description_ignores_internal_table_metadata_description() -> None:
    origin_without_desc = Origin(producer="Producer", title="Original")
    table = TableSchemaInput(
        short_name="t",
        metadata=TableMeta(short_name="t", title="T"),
        variables={"value": VariableMeta(title="Value", origins=[origin_without_desc])},
        formats=["feather"],
    )
    # No origin/dataset description available anywhere -> None (nothing is synthesized).
    assert table_description(table, DatasetMeta(short_name="d")) is None
    # TableMeta.description is a mostly-internal field and must never leak into JSON-LD, even
    # when explicitly set: it neither wins over an available dataset description...
    table.metadata.description = "Internal table description"
    assert table_description(table, DatasetMeta(short_name="d", description="ds")) == "ds"
    # ...nor gets used as a last resort when nothing else is available.
    assert table_description(table, DatasetMeta(short_name="d")) is None


def test_dataset_description_raises_without_explicit_description() -> None:
    """Regression guard: a dataset with no dataset- or table-level description must fail loudly
    rather than silently borrow a description from one of its indicators' origins. Previously, a
    dataset combining many indicators from its own primary source with a borrowed auxiliary
    indicator (e.g. population, added for per-capita columns) would describe itself using
    whichever origin happened to be attached to the first column defined in the table — here,
    population's origin — even though population has nothing to do with the dataset's actual
    subject matter."""
    population_origin = Origin(producer="Our World in Data", title="Population", description="Population blurb.")
    energy_origin = Origin(producer="Energy Institute", title="Statistical Review", description="Energy blurb.")
    table = TableSchemaInput(
        short_name="owid_energy",
        metadata=TableMeta(short_name="owid_energy", title="Energy"),
        variables={
            "population": VariableMeta(title="Population", origins=[population_origin]),
            "coal_consumption": VariableMeta(title="Coal consumption", origins=[energy_origin]),
        },
        formats=["feather"],
    )

    try:
        dataset_to_schema_org(
            dataset_path="garden/energy_data/2026-04-24/owid_energy",
            page_path="energy_data/owid_energy",
            dataset_meta=DatasetMeta(namespace="energy_data", version="2026-04-24", short_name="owid_energy"),
            tables=[table],
        )
        raise AssertionError("Expected dataset_to_schema_org to raise ValueError for a missing description.")
    except ValueError as error:
        assert "owid_energy" in str(error)


def test_dataset_description_ignores_table_level_description() -> None:
    """A table's own ``description`` (a mostly-internal field) must never substitute for a real
    ``dataset.description`` — only an explicit dataset-level description satisfies the
    requirement, even for a single-table dataset."""
    table = TableSchemaInput(
        short_name="owid_energy",
        metadata=TableMeta(short_name="owid_energy", title="Energy", description="Internal table description."),
        variables={
            "population": VariableMeta(
                title="Population",
                origins=[Origin(producer="Our World in Data", title="Population", description="Population blurb.")],
            )
        },
        formats=["feather"],
    )

    try:
        dataset_to_schema_org(
            dataset_path="garden/energy_data/2026-04-24/owid_energy",
            page_path="energy_data/owid_energy",
            dataset_meta=DatasetMeta(namespace="energy_data", version="2026-04-24", short_name="owid_energy"),
            tables=[table],
        )
        raise AssertionError("Expected dataset_to_schema_org to raise ValueError for a missing description.")
    except ValueError as error:
        assert "owid_energy" in str(error)


def test_license_to_url_resolves_known_license_names() -> None:
    assert license_to_url(License(name="CC BY 4.0")) == "https://creativecommons.org/licenses/by/4.0/"
    assert license_to_url(License(name=" CC-BY 4.0 ")) == "https://creativecommons.org/licenses/by/4.0/"
    assert license_to_url(License(name="Custom license")) is None

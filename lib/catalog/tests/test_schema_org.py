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
    assert jsonld["creator"] == {
        "@type": "Organization",
        "name": "Our World in Data",
        "url": "https://ourworldindata.org",
    }
    # citation is deliberately never emitted; the producer is credited via isBasedOn below.
    assert "citation" not in jsonld
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


def test_templated_variable_metadata_is_never_emitted_raw() -> None:
    meta = VariableMeta(
        title='<% if poverty_line == "215" %>Below $2.15<% else %>Below the line<% endif %>',
        description_short="The share of population below <<poverty_line>> a day.",
        unit="international-$ in <<ppp_version>> prices",
    )
    table = TableSchemaInput(
        short_name="poverty",
        metadata=TableMeta(short_name="poverty", title="Poverty", description="Table description"),
        variables={"headcount_ratio": meta},
        formats=["feather"],
    )
    jsonld = dataset_to_schema_org(
        dataset_path="garden/wb/2025-01-01/pip",
        page_path="wb/pip",
        dataset_meta=DatasetMeta(short_name="pip", title="PIP", description="Dataset description"),
        tables=[table],
    )
    (variable,) = jsonld["variableMeasured"]
    # Without representative dimension values nothing can be rendered: fall back to the
    # identifier and omit the templated fields entirely.
    assert variable["name"] == "headcount_ratio"
    assert "description" not in variable
    assert "unitText" not in variable
    serialized = str(jsonld)
    assert "<%" not in serialized and "<<" not in serialized


def test_templated_description_renders_example_using_representative_dimensions() -> None:
    meta = VariableMeta(
        title="Poverty headcount",
        description_short='People below <% if poverty_line == "215" %>$2.15<% else %>the line<% endif %> a day.',
        unit="people in <<ppp_version>> prices",
    )
    table = TableSchemaInput(
        short_name="poverty",
        metadata=TableMeta(
            short_name="poverty",
            title="Poverty",
            description="Table description",
            dimensions=[
                {"name": "country", "slug": "country"},
                {"name": "year", "slug": "year"},
                {"name": "Poverty line", "slug": "poverty_line"},
            ],
        ),
        variables={"headcount": meta},
        formats=["feather"],
        primary_key=["country", "year", "poverty_line"],
        dimension_values={"poverty_line": ["215", "365"]},
        representative_dimensions={"poverty_line": "215"},
    )
    jsonld = dataset_to_schema_org(
        dataset_path="garden/wb/2025-01-01/pip",
        page_path="wb/pip",
        dataset_meta=DatasetMeta(short_name="pip", title="PIP", description="Dataset description"),
        tables=[table],
    )
    variables = {variable["identifier"]: variable for variable in jsonld["variableMeasured"]}
    # country/year are conveyed by temporal/spatialCoverage, not emitted as dimensions.
    assert set(variables) == {"poverty_line", "headcount"}
    assert variables["poverty_line"]["name"] == "Poverty line"
    assert "Values: 215, 365." in variables["poverty_line"]["description"]
    assert variables["headcount"]["description"] == (
        "For example, for poverty_line=215: People below $2.15 a day. Varies by the dimension columns: poverty_line."
    )
    # A templated unit is only correct for one slice of the column: omitted.
    assert "unitText" not in variables["headcount"]


def test_plain_description_strips_detail_on_demand_links() -> None:
    meta = VariableMeta(title="Consumption", description_short="Measured in [terawatt-hours](#dod:watt-hours).")
    table = TableSchemaInput(
        short_name="energy",
        metadata=TableMeta(short_name="energy", title="Energy", description="Table description"),
        variables={"consumption": meta},
        formats=["feather"],
    )
    jsonld = dataset_to_schema_org(
        dataset_path="garden/energy/2025-01-01/energy",
        page_path="energy/energy",
        dataset_meta=DatasetMeta(short_name="energy", title="Energy", description="Dataset description"),
        tables=[table],
    )
    (variable,) = jsonld["variableMeasured"]
    # Detail-on-demand links only resolve on ourworldindata.org; keep just the link text.
    assert variable["description"] == "Measured in terawatt-hours."


def test_no_citation_emitted_and_is_based_on_leads_with_most_used_origin() -> None:
    main_origin = Origin(
        producer="Global Carbon Project",
        attribution="Global Carbon Budget (2025)",
        title="Global Carbon Budget",
        citation_full=(
            "Friedlingstein, P. et al.: Global Carbon Budget 2024, Earth Syst. Sci. Data, "
            "https://doi.org/10.5194/essd-17-965-2025, 2025."
        ),
        url_main="https://globalcarbonbudget.org",
    )
    helper_origin = Origin(
        producer="Various sources",
        attribution="Population based on various sources (2024)",
        title="Population",
        citation_full="Population is based on various sources: https://ourworldindata.org/population-sources",
        url_main="https://example.com/population",
    )
    # The helper origin backs the *first* column; the main origin backs the remaining
    # columns. isBasedOn must still lead with the main origin, not follow column order.
    variables = {"population": VariableMeta(title="Population", origins=[helper_origin])}
    for name in ("co2", "co2_per_capita"):
        variables[name] = VariableMeta(title=name, origins=[main_origin])
    table = TableSchemaInput(
        short_name="owid_co2",
        metadata=TableMeta(short_name="owid_co2", title="CO2", description="Table description"),
        variables=variables,
        formats=["feather"],
    )
    jsonld = dataset_to_schema_org(
        dataset_path="garden/emissions/2025-12-04/owid_co2",
        page_path="emissions/owid_co2",
        dataset_meta=DatasetMeta(short_name="owid_co2", title="CO2 dataset", description="Dataset description"),
        tables=[table],
    )
    # Creator is the author of this compiled artifact; producers keep credit in isBasedOn.
    assert jsonld["creator"] == {
        "@type": "Organization",
        "name": "Our World in Data",
        "url": "https://ourworldindata.org",
    }
    # citation is deliberately never emitted, even though the main origin's citation_full
    # carries a DOI: a mined citation list kept misrepresenting auxiliary sources as the
    # dataset's primary reference. isBasedOn carries the source credit.
    assert "citation" not in jsonld
    assert jsonld["isBasedOn"][0]["url"] == "https://globalcarbonbudget.org"
    assert {item["url"] for item in jsonld["isBasedOn"]} >= {"https://example.com/population"}


def test_dataset_level_license_wins_over_origin_license() -> None:
    """A dataset-level license declared in .meta.yml describes the compiled artifact and must
    take precedence over per-source origin licenses (owid_co2 used to advertise GCB's ICOS
    data license just because GCB is its most-referenced origin)."""
    origin = Origin(
        producer="Global Carbon Project",
        title="Global Carbon Budget",
        license=License(name="ICOS", url="https://www.icos-cp.eu/data-services/about-data-portal/data-license"),
    )
    table = TableSchemaInput(
        short_name="owid_co2",
        metadata=TableMeta(short_name="owid_co2", title="CO2", description="Table description"),
        variables={"co2": VariableMeta(title="CO2", origins=[origin])},
        formats=["feather"],
    )
    kwargs: dict = dict(
        dataset_path="garden/emissions/2025-12-04/owid_co2",
        page_path="emissions/owid_co2",
        tables=[table],
    )

    jsonld = dataset_to_schema_org(
        dataset_meta=DatasetMeta(
            short_name="owid_co2",
            title="CO2 dataset",
            description="Dataset description",
            licenses=[License(name="CC BY 4.0", url="https://creativecommons.org/licenses/by/4.0/")],
        ),
        **kwargs,
    )
    assert jsonld["license"] == "https://creativecommons.org/licenses/by/4.0/"

    # Without a dataset-level license, the most-referenced origin's license is the fallback.
    jsonld = dataset_to_schema_org(
        dataset_meta=DatasetMeta(short_name="owid_co2", title="CO2 dataset", description="Dataset description"),
        **kwargs,
    )
    assert jsonld["license"] == "https://www.icos-cp.eu/data-services/about-data-portal/data-license"


def test_keywords_ordered_by_variable_count_not_column_order() -> None:
    """keywords[0] is treated as the dataset's primary topic (e.g. by the landing page's
    charts link), so the most-tagged topic must lead even when a helper column comes first."""
    gdp_tag = VariablePresentationMeta(topic_tags=["Economic Growth"])
    co2_tag = VariablePresentationMeta(topic_tags=["CO2 & Greenhouse Gas Emissions"])
    origin = Origin(producer="Example Producer", title="Origin title")
    variables = {"gdp": VariableMeta(title="gdp", origins=[origin], presentation=gdp_tag)}
    for name in ("co2", "co2_per_capita", "cumulative_co2"):
        variables[name] = VariableMeta(title=name, origins=[origin], presentation=co2_tag)
    table = TableSchemaInput(
        short_name="owid_co2",
        metadata=TableMeta(short_name="owid_co2", title="CO2"),
        variables=variables,
        formats=["feather"],
    )
    jsonld = dataset_to_schema_org(
        dataset_path="garden/emissions/2025-12-04/owid_co2",
        page_path="emissions/owid_co2",
        dataset_meta=DatasetMeta(short_name="owid_co2", title="CO2 dataset", description="Dataset description"),
        tables=[table],
    )
    assert jsonld["keywords"] == ["CO2 & Greenhouse Gas Emissions", "Economic Growth"]

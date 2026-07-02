from owid.catalog.core.meta import DatasetMeta, License, Origin, TableMeta, VariableMeta
from owid.catalog.schema_org import TableSchemaInput

from etl.catalog_jsonld.quality import assess_dataset_quality, find_duplicate_short_key_paths, is_reserved_namespace


def _eligible_table() -> TableSchemaInput:
    origin = Origin(
        producer="Example Producer",
        title="Original dataset",
        description="Original description",
        citation_full="Example Producer (2025). Original dataset.",
        license=License(name="CC BY 4.0"),
    )
    return TableSchemaInput(
        short_name="example_table",
        metadata=TableMeta(short_name="example_table", title="Example table", description="Table description"),
        variables={"value": VariableMeta(title="Value", description_short="A measured value.", origins=[origin])},
        formats=["feather"],
    )


def _eligible_dataset_meta(**overrides: object) -> DatasetMeta:
    fields: dict[str, object] = dict(
        channel="garden",
        namespace="example",
        version="2025-01-01",
        short_name="example_dataset",
        title="Example dataset",
        description="Dataset description",
    )
    fields.update(overrides)
    return DatasetMeta(**fields)  # type: ignore[arg-type]


def _table(short_name: str, *, table_desc: str | None = None, origin_desc: str | None = None) -> TableSchemaInput:
    origin = Origin(
        producer="Example Producer",
        title="Original dataset",
        description=origin_desc,
        url_main="https://example.com/source",
        license=License(name="CC BY 4.0"),
    )
    return TableSchemaInput(
        short_name=short_name,
        metadata=TableMeta(short_name=short_name, title=short_name, description=table_desc),
        variables={"value": VariableMeta(title="Value", origins=[origin])},
        formats=["feather"],
    )


def test_is_reserved_namespace_matches_channel_names() -> None:
    assert is_reserved_namespace("garden")
    assert is_reserved_namespace("snapshot")
    assert is_reserved_namespace("open_numbers")
    assert not is_reserved_namespace("emissions")


def test_is_reserved_namespace_matches_root_level_files() -> None:
    assert is_reserved_namespace("robots.txt")
    assert is_reserved_namespace("jsonld_quality_report.json")


def test_is_reserved_namespace_matches_sitemap_family() -> None:
    assert is_reserved_namespace("sitemap.xml")
    assert is_reserved_namespace("sitemap-index.xml")
    assert is_reserved_namespace("sitemap-1.xml")
    assert not is_reserved_namespace("sitemapper")  # doesn't end in .xml


def test_assess_dataset_quality_blocks_reserved_namespace() -> None:
    result = assess_dataset_quality(
        catalog_path="garden/garden/2025-01-01/something",
        namespace="garden",
        dataset_meta=_eligible_dataset_meta(namespace="garden", short_name="something"),
        tables=[_eligible_table()],
    )

    assert "reserved_namespace" in result.blockers
    assert not result.is_eligible


def test_assess_dataset_quality_allows_non_reserved_namespace() -> None:
    result = assess_dataset_quality(
        catalog_path="garden/emissions/2025-01-01/owid_co2",
        namespace="emissions",
        dataset_meta=_eligible_dataset_meta(namespace="emissions", short_name="owid_co2"),
        tables=[_eligible_table()],
    )

    assert "reserved_namespace" not in result.blockers
    assert result.is_eligible


def test_assess_dataset_quality_blocks_duplicate_short_key() -> None:
    result = assess_dataset_quality(
        catalog_path="garden/emissions/2025-01-01/owid_co2",
        namespace="emissions",
        dataset_meta=_eligible_dataset_meta(),
        tables=[_eligible_table()],
        duplicate_short_key=True,
    )

    assert "duplicate_short_key" in result.blockers
    assert not result.is_eligible


def test_find_duplicate_short_key_paths_flags_shared_namespace_dataset_pairs() -> None:
    entries = [
        ("garden/emissions/2025-01-01/owid_co2", "emissions", "owid_co2"),
        ("open_numbers/emissions/2025-01-01/owid_co2", "emissions", "owid_co2"),
        ("garden/wb/2026-03-24/world_bank_pip", "wb", "world_bank_pip"),
    ]

    duplicates = find_duplicate_short_key_paths(entries)

    assert duplicates == {"garden/emissions/2025-01-01/owid_co2", "open_numbers/emissions/2025-01-01/owid_co2"}


def test_find_duplicate_short_key_paths_empty_when_all_unique() -> None:
    entries = [
        ("garden/emissions/2025-01-01/owid_co2", "emissions", "owid_co2"),
        ("garden/wb/2026-03-24/world_bank_pip", "wb", "world_bank_pip"),
    ]

    assert find_duplicate_short_key_paths(entries) == set()


def test_missing_table_description_not_warned_when_dataset_description_present() -> None:
    # Neither the tables nor their origins carry a description, but the dataset does — the
    # emitted JSON-LD falls back to it, so the warning must not fire.
    result = assess_dataset_quality(
        catalog_path="garden/example/2025-01-01/example_dataset",
        namespace="example",
        dataset_meta=DatasetMeta(short_name="example_dataset", title="Example", description="Dataset description"),
        tables=[_table("table_a"), _table("table_b")],
    )
    assert result.is_eligible
    assert result.table_warnings == {}


def test_assess_dataset_quality_blocks_missing_description_even_with_origin_description() -> None:
    # An indicator origin's description (e.g. a borrowed population/GDP column) must not count as
    # the dataset having a description of its own — only an explicit dataset- or table-level one
    # does. Otherwise a dataset could pass this gate and then blow up in dataset_to_schema_org,
    # which refuses to guess a description from origins.
    result = assess_dataset_quality(
        catalog_path="garden/example/2025-01-01/example_dataset",
        namespace="example",
        dataset_meta=DatasetMeta(short_name="example_dataset", title="Example"),
        tables=[_table("table_a", origin_desc="Producer description of the data.")],
    )
    assert "missing_description" in result.blockers
    assert not result.is_eligible


def test_missing_table_description_not_warned_when_origin_description_present() -> None:
    # No table or dataset description, but the producer (origin) provides one.
    result = assess_dataset_quality(
        catalog_path="garden/example/2025-01-01/example_dataset",
        namespace="example",
        dataset_meta=DatasetMeta(short_name="example_dataset", title="Example"),
        tables=[_table("table_a", origin_desc="Producer description of the data.")],
    )
    assert "table_a" not in result.table_warnings


def test_missing_table_description_warned_when_no_description_anywhere() -> None:
    # No description on the table, its origins, or the dataset -> the warning is a real signal.
    result = assess_dataset_quality(
        catalog_path="garden/example/2025-01-01/example_dataset",
        namespace="example",
        dataset_meta=DatasetMeta(short_name="example_dataset", title="Example"),
        tables=[_table("table_a")],
    )
    assert result.table_warnings["table_a"] == ["missing_table_description"]

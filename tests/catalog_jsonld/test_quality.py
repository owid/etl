from owid.catalog.core.meta import DatasetMeta, License, Origin, TableMeta, VariableMeta
from owid.catalog.schema_org import TableSchemaInput

from etl.catalog_jsonld.quality import assess_dataset_quality


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


def test_missing_table_description_not_warned_when_dataset_description_present() -> None:
    # Neither the tables nor their origins carry a description, but the dataset does — the
    # emitted JSON-LD falls back to it, so the warning must not fire.
    result = assess_dataset_quality(
        catalog_path="garden/example/2025-01-01/example_dataset",
        dataset_meta=DatasetMeta(short_name="example_dataset", title="Example", description="Dataset description"),
        tables=[_table("table_a"), _table("table_b")],
    )
    assert result.is_eligible
    assert result.table_warnings == {}


def test_missing_table_description_not_warned_when_origin_description_present() -> None:
    # No table or dataset description, but the producer (origin) provides one.
    result = assess_dataset_quality(
        catalog_path="garden/example/2025-01-01/example_dataset",
        dataset_meta=DatasetMeta(short_name="example_dataset", title="Example"),
        tables=[_table("table_a", origin_desc="Producer description of the data.")],
    )
    assert "table_a" not in result.table_warnings


def test_missing_table_description_warned_when_no_description_anywhere() -> None:
    # No description on the table, its origins, or the dataset -> the warning is a real signal.
    result = assess_dataset_quality(
        catalog_path="garden/example/2025-01-01/example_dataset",
        dataset_meta=DatasetMeta(short_name="example_dataset", title="Example"),
        tables=[_table("table_a")],
    )
    assert result.table_warnings["table_a"] == ["missing_table_description"]

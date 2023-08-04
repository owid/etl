#
#  test_tables.py
#

import json
import tempfile
from os.path import exists, join, splitext

import jsonschema
import numpy as np
import pandas as pd
import pytest

from owid.catalog import tables
from owid.catalog.datasets import FileFormat
from owid.catalog.meta import TableMeta, VariableMeta
from owid.catalog.tables import SCHEMA, Table, get_unique_sources_from_tables
from owid.catalog.variables import PROCESSING_LOG, Variable

from .mocking import mock


def test_create():
    t = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})
    assert list(t.gdp) == [100, 102, 104]
    assert list(t.country) == ["AU", "SE", "CH"]


def test_create_with_underscore():
    t = Table({"GDP": [100, 102, 104]}, underscore=True, short_name="GDP Table")
    assert t.columns == ["gdp"]
    assert t.metadata.short_name == "gdp_table"


def test_add_table_metadata():
    t = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})

    # write some metadata
    t.metadata.short_name = "my_table"
    t.metadata.title = "My table indeed"
    t.metadata.description = "## Well...\n\nI discovered this table in the Summer of '63..."

    # metadata persists with slicing
    t2 = t.iloc[:2]
    assert t2.metadata == t.metadata


def test_read_empty_table_metadata():
    t = Table()
    assert t.metadata == TableMeta()


def test_table_schema_is_valid():
    jsonschema.Draft7Validator.check_schema(SCHEMA)


def test_add_field_metadata():
    t = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})
    title = "GDP per capita in 2011 international $"

    assert t.gdp.metadata == VariableMeta()

    t.gdp.title = title

    # check single field access
    assert t.gdp.title == title

    # check entire metadata access
    assert t.gdp.metadata == VariableMeta(title=title)

    # check field-level metadata persists across slices
    assert t.iloc[:1].gdp.title == title


def test_can_overwrite_column_with_apply():
    table = Table({"a": [1, 2, 3], "b": [4, 5, 6]})
    table.a.metadata.title = "This thing is a"

    v = table.a.apply(lambda x: x + 1)
    assert v.name is not None
    assert v.metadata.title == "This thing is a"

    table["a"] = v
    assert table.a.tolist() == [2, 3, 4]


def test_saving_empty_table_fails():
    t = Table()

    with pytest.raises(Exception):
        t.to_feather("/tmp/example.feather")


# The parametrize decorator runs this test multiple times with different formats
@pytest.mark.parametrize("format", ["csv", "feather", "parquet"])
def test_round_trip_no_metadata(format: FileFormat) -> None:
    t1 = Table({"gdp": [100, 102, 104, 100], "countries": ["AU", "SE", "NA", "💡"]})
    with tempfile.TemporaryDirectory() as path:
        filename = join(path, f"table.{format}")
        t1.to(filename)

        assert exists(filename)
        if format in ["csv", "feather"]:
            assert exists(splitext(filename)[0] + ".meta.json")

        t2 = Table.read(filename)
        assert_tables_eq(t1, t2)


@pytest.mark.parametrize("format", ["csv", "feather", "parquet"])
def test_round_trip_with_index(format: FileFormat) -> None:
    t1 = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "NA"]})
    t1.set_index("country", inplace=True)
    with tempfile.TemporaryDirectory() as path:
        filename = join(path, f"table.{format}")
        t1.to(filename)

        assert exists(filename)
        if format in ["csv", "feather"]:
            assert exists(splitext(filename)[0] + ".meta.json")

        t2 = Table.read(filename)
        assert_tables_eq(t1, t2)


@pytest.mark.parametrize("format", ["csv", "feather", "parquet"])
def test_round_trip_with_metadata(format: FileFormat) -> None:
    t1 = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "NA"]})
    t1.set_index("country", inplace=True)
    t1.title = "A very special table"
    t1.description = "Something something"

    with tempfile.TemporaryDirectory() as path:
        filename = join(path, f"table.{format}")
        t1.to(filename)

        assert exists(filename)
        if format in ["csv", "feather"]:
            assert exists(splitext(filename)[0] + ".meta.json")

        t2 = Table.read(filename)
        assert_tables_eq(t1, t2)


def test_field_metadata_copied_between_tables():
    t1 = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})
    t2 = Table({"hdi": [73, 92, 45], "country": ["AU", "SE", "CH"]})

    t1.gdp.description = "A very important measurement"

    t2["gdp"] = t1.gdp
    assert t2.gdp.metadata == t1.gdp.metadata


def test_field_metadata_serialised():
    t1 = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})
    t1.gdp.description = "Something grand"

    with tempfile.TemporaryDirectory() as dirname:
        filename = join(dirname, "test.feather")
        t1.to_feather(filename)

        t2 = Table.read_feather(filename)
        assert_tables_eq(t1, t2)


def test_tables_from_dataframes_have_variable_columns():
    df = pd.DataFrame({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})
    t = Table(df)
    assert isinstance(t.gdp, Variable)

    t.gdp.metadata.title = "test"


def test_tables_always_list_fields_in_metadata():
    df = pd.DataFrame(
        {
            "gdp": [100, 102, 104],
            "country": ["AU", "SE", "CH"],
            "french_fries": ["yes", "no", "yes"],
        }
    )
    t = Table(df.set_index("country"))
    with tempfile.TemporaryDirectory() as temp_dir:
        t.to_feather(join(temp_dir, "example.feather"))
        m = json.load(open(join(temp_dir, "example.meta.json")))

    assert m["primary_key"] == ["country"]
    assert m["fields"] == {
        "country": {},
        "french_fries": {},
        "gdp": {},
    }


def test_field_access_can_be_typecast():
    # https://github.com/owid/owid-catalog-py/issues/12
    t = mock_table()
    t.gdp.metadata.description = "One two three"
    v = t.gdp.astype("object")
    t["gdp"] = v
    assert t.gdp.metadata.description == "One two three"


def test_tables_can_drop_duplicates():
    # https://github.com/owid/owid-catalog-py/issues/11
    t: Table = Table({"gdp": [100, 100, 102, 104], "country": ["AU", "AU", "SE", "CH"]}).set_index(
        "country"
    )  # type: ignore
    t.metadata = mock(TableMeta)

    # in the bug, the dtype of t.duplicated() became object
    dups = t.duplicated()
    assert dups.dtype == np.bool_

    # this caused drop_duplicates() to fail
    t2 = t.drop_duplicates()

    assert isinstance(t2, Table)


def test_extra_fields_ignored_in_metadata() -> None:
    metadata = {"dog": 1, "sheep": [1, 2, 3], "llama": "Sam"}
    table_meta = TableMeta.from_dict(metadata)
    assert table_meta


def assert_tables_eq(lhs: Table, rhs: Table) -> None:
    assert lhs.to_dict() == rhs.to_dict()
    assert lhs.metadata == rhs.metadata
    assert lhs._fields == rhs._fields


def mock_table() -> Table:
    t: Table = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]}).set_index("country")  # type: ignore
    t.metadata = mock(TableMeta)
    t.metadata.primary_key = ["country"]
    for col in t.all_columns:
        t._fields[col] = mock(VariableMeta)

    return t


def test_load_csv_table_over_http() -> None:
    Table.read_csv("http://owid-catalog.nyc3.digitaloceanspaces.com/reference/countries_regions.csv")


def test_rename_columns() -> None:
    t: Table = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]}).set_index("country")  # type: ignore
    t.gdp.metadata.title = "GDP"
    new_t = t.rename(columns={"gdp": "new_gdp"})
    assert new_t.new_gdp.metadata.title == "GDP"
    assert new_t.columns == ["new_gdp"]

    # old table hasn't changed
    assert t.gdp.metadata.title == "GDP"


def test_rename_columns_inplace() -> None:
    t: Table = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]}).set_index("country")  # type: ignore
    t.gdp.metadata.title = "GDP"
    t.rename(columns={"gdp": "new_gdp"}, inplace=True)
    assert t.new_gdp.metadata.title == "GDP"
    assert t.columns == ["new_gdp"]


def test_copy() -> None:
    t: Table = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]}).set_index("country")  # type: ignore
    t.metadata.title = "GDP table"
    t.gdp.metadata.title = "GDP"
    t2 = t.copy()

    t2.metadata.title = "GDP table copy"
    t2.gdp.metadata.title = "GDP copy"
    t2.reset_index(inplace=True)

    assert t.gdp.metadata.title == "GDP"
    assert t.metadata.title == "GDP table"
    assert t.primary_key == ["country"]

    assert t2.gdp.metadata.title == "GDP copy"
    assert t2.metadata.title == "GDP table copy"
    assert t2.primary_key == []


def test_copy_metadata_from() -> None:
    t: Table = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})
    t.metadata.title = "GDP table"
    t.gdp.metadata.title = "GDP"

    t2: Table = Table(pd.DataFrame(t))
    t2.country.metadata.title = "Country"

    t2.copy_metadata_from(t)

    assert t2.gdp.metadata.title == "GDP"
    assert t2.country.metadata.title == "Country"
    assert t2.metadata.title == "GDP table"


def test_addition_without_metadata() -> None:
    t: Table = Table({"a": [1, 2], "b": [3, 4]})
    t["c"] = t["a"] + t["b"]
    if PROCESSING_LOG:
        expected_metadata = VariableMeta(processing_log=[{"variable": "c", "parents": ["a", "b"], "operation": "+"}])
    else:
        expected_metadata = VariableMeta()
    assert t.c.metadata == expected_metadata


def test_addition_with_metadata() -> None:
    t: Table = Table({"a": [1, 2], "b": [3, 4]})
    t.a.metadata.title = "A"
    t.b.metadata.title = "B"

    t["c"] = t["a"] + t["b"]

    if PROCESSING_LOG:
        expected_metadata = VariableMeta(processing_log=[{"variable": "c", "parents": ["a", "b"], "operation": "+"}])
    else:
        expected_metadata = VariableMeta()
    assert t.c.metadata == expected_metadata

    t.c.metadata.title = "C"

    # addition shouldn't change the metadata of the original columns
    assert t.a.metadata.title == "A"
    assert t.b.metadata.title == "B"
    assert t.c.metadata.title == "C"


def test_addition_same_variable() -> None:
    t: Table = Table({"a": [1, 2], "b": [3, 4]})
    t.a.metadata.title = "A"
    t.b.metadata.title = "B"

    t["a"] = t["a"] + t["b"]

    # Now variable "a" has a different meaning, so the title should not be preserved (but "b"'s title should).
    assert t.a.metadata.title is None
    assert t.b.metadata.title == "B"


def test_addition_of_scalar() -> None:
    t: Table = Table({"a": [1, 2], "b": [3, 4]})
    t.a.metadata.title = "A"
    t.b.metadata.title = "B"

    t["a"] = t["a"] + 1

    # Adding a scalar should not affect the variable's metadata (except the processing log).
    assert t.a.metadata.title == "A"
    assert t.b.metadata.title == "B"


def test_set_index_keeps_metadata() -> None:
    tb = Table(pd.DataFrame({"a": [1, 2], "b": [3, 4]}))
    tb["a"].metadata.title = "A"
    tb["b"].metadata.title = "B"

    tb_new = tb.set_index(["a"])
    tb_new = tb_new.reset_index()

    # metadata should be preserved
    assert tb_new["a"].metadata.title == "A"
    assert tb_new["b"].metadata.title == "B"


def test_set_index_keeps_metadata_inplace() -> None:
    tb = Table(pd.DataFrame({"a": [1, 2], "b": [3, 4]}))
    tb["a"].metadata.title = "A"
    tb["b"].metadata.title = "B"

    tb_new = tb.set_index(["a"])
    tb_new.reset_index(inplace=True)

    # metadata should be preserved
    assert tb_new["a"].metadata.title == "A"
    assert tb_new["b"].metadata.title == "B"


def test_assign_dataset_sources_and_licenses_to_each_variable(table_1, sources) -> None:
    tb = table_1.copy()
    # Create a new variable without metadata.
    tb["c"] = 1
    tb = tables.assign_dataset_sources_and_licenses_to_each_variable(tb)
    # Check that variables that did not have sources now have the dataset of the dataset.
    assert tb["c"].metadata.sources == [sources[1], sources[2], sources[3]]
    # Check that variables that did have sources were not affected.
    assert tb["a"].metadata.sources == [sources[2], sources[1]]
    # Note: This function will also add all sources to columns "country" and "year", which may not be a desired effect.


def test_merge_without_any_on_arguments(table_1, table_2, sources, licenses) -> None:
    # If "on", "left_on" and "right_on" are not specified, the join is performed on common columns.
    # In this case, "country", "year", "a".
    tb = tables.merge(table_1, table_2)
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.merge(table_2))
    # Check that non-overlapping columns preserve metadata.
    assert tb["c"].metadata == table_2["c"].metadata
    # Check that "on" columns combine the metadata of left and right tables.
    # Column "country" has the same title on both tables, but has description only on table_1, therefore when combining
    # with table_2, title should be preserved, but description will be lost.
    assert tb["country"].metadata.title == "Country Title"
    assert tb["country"].metadata.description is None
    # Column "year" has no metadata in either table.
    assert tb["year"].metadata.title is None
    assert tb["year"].metadata.description is None
    # Column "a" should combine the sources and licenses of of both tables (not title and description, since they are
    # different in left and right tables).
    assert tb["a"].metadata.title is None
    assert tb["a"].metadata.description is None
    assert tb["a"].metadata.sources == [sources[2], sources[1]]
    assert tb["a"].metadata.licenses == [licenses[1], licenses[2]]
    # Column "b" appears only in table_1, so it should keep its original metadata.
    assert tb["b"].metadata == table_1["b"].metadata
    # Column "c" appears only in table_2, so it should keep its original metadata.
    assert tb["c"].metadata == table_2["c"].metadata
    # Now check the table metadata.
    # Since titles and descriptions of the tables concatenated are different, title and description should be empty.
    assert tb.metadata.title is None
    assert tb.metadata.description is None
    # Sources and licenses should be all sources and licenses of the resulting variables.
    # assert tb.metadata.dataset.sources == [sources[2], sources[1], sources[3], sources[4]]  # type: ignore
    # assert tb.metadata.dataset.licenses == [licenses[1], licenses[2], licenses[3], licenses[4]]  # type: ignore


def test_merge_with_on_argument(table_1, table_2, sources, licenses) -> None:
    tb = tables.merge(table_1, table_2, on=["country", "year"])
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.merge(table_2, on=["country", "year"]))
    # Column "country" has the same title on both tables, but has description only on table_1, therefore when combining
    # with table_2, title should be preserved, but description will be lost.
    assert tb["country"].metadata.title == "Country Title"
    assert tb["country"].metadata.description is None
    # Idem for "year".
    assert tb["year"].metadata.title is None
    assert tb["year"].metadata.description is None
    # Given that there is a column "a" in both left and right tables, they will be renamed as "a_x" and "a_y" in the
    # merged table. Then, "a_x" should have the metadata of the left table, and "a_y" should have the metadata of the
    # right table.
    assert tb["a_x"].metadata == table_1["a"].metadata
    assert tb["a_y"].metadata == table_2["a"].metadata
    # Column "b" appears only in table_1, so it should keep its original metadata.
    assert tb["b"].metadata == table_1["b"].metadata
    # Column "c" appears only in table_2, so it should keep its original metadata.
    assert tb["c"].metadata == table_2["c"].metadata
    # Idem but specifying suffixes.
    tb = tables.merge(table_1, table_2, on=["country", "year"], suffixes=("_left", "_right"))
    assert tb["a_left"].metadata == table_1["a"].metadata
    assert tb["a_right"].metadata == table_2["a"].metadata
    # Now check the table metadata.
    # Since titles and descriptions of the tables concatenated are different, title and description should be empty.
    assert tb.metadata.title is None
    assert tb.metadata.description is None
    # Sources and licenses should be all sources and licenses of the resulting variables.
    # assert tb.metadata.dataset.sources == [sources[2], sources[1], sources[3], sources[4]]  # type: ignore
    # assert tb.metadata.dataset.licenses == [licenses[1], licenses[2], licenses[3], licenses[4]]  # type: ignore


def test_merge_with_left_on_and_right_on_argument(table_1, table_2, sources, licenses) -> None:
    # Join on columns "country" and "year" (in this case, the result should be identical to simply defining
    # on=["country", "year"]).
    tb = tables.merge(table_1, table_2, left_on=["country", "year"], right_on=["country", "year"])
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.merge(table_2, left_on=["country", "year"], right_on=["country", "year"]))
    # Column "country" has the same title on both tables, but has description only on table_1, therefore when combining
    # with table_2, title should be preserved, but description will be lost.
    assert tb["country"].metadata.title == "Country Title"
    assert tb["country"].metadata.description is None
    # Idem for "year".
    assert tb["year"].metadata.title is None
    assert tb["year"].metadata.description is None
    # Given that there is a column "a" in both left and right tables, they will be renamed as "a_x" and "a_y" in the
    # merged table. Then, "a_x" should have the metadata of the left table, and "a_y" should have the metadata of the
    # right table.
    assert tb["a_x"].metadata == table_1["a"].metadata
    assert tb["a_y"].metadata == table_2["a"].metadata
    # Column "b" appears only in table_1, so it should keep its original metadata.
    assert tb["b"].metadata == table_1["b"].metadata
    # Column "c" appears only in table_2, so it should keep its original metadata.
    assert tb["c"].metadata == table_2["c"].metadata
    # Now check the table metadata.
    # Since titles and descriptions of the tables concatenated are different, title and description should be empty.
    assert tb.metadata.title is None
    assert tb.metadata.description is None
    # Sources and licenses should be all sources and licenses of the resulting variables.
    # assert tb.metadata.dataset.sources == [sources[2], sources[1], sources[3], sources[4]]  # type: ignore
    # assert tb.metadata.dataset.licenses == [licenses[1], licenses[2], licenses[3], licenses[4]]  # type: ignore

    # Repeat the same merge, but specifying suffixes.
    tb = tables.merge(
        table_1, table_2, left_on=["country", "year"], right_on=["country", "year"], suffixes=("_left", "_right")
    )
    # Check that the result is identical to using the table method.
    assert tb.equals_table(
        table_1.merge(table_2, left_on=["country", "year"], right_on=["country", "year"], suffixes=("_left", "_right"))
    )
    assert tb["a_left"].metadata == table_1["a"].metadata
    assert tb["a_right"].metadata == table_2["a"].metadata
    # Now check the table metadata.
    # Since titles and descriptions of the tables concatenated are different, title and description should be empty.
    assert tb.metadata.title is None
    assert tb.metadata.description is None
    # Sources and licenses should be all sources and licenses of the resulting variables.
    # assert tb.metadata.dataset.sources == [sources[2], sources[1], sources[3], sources[4]]  # type: ignore
    # assert tb.metadata.dataset.licenses == [licenses[1], licenses[2], licenses[3], licenses[4]]  # type: ignore

    # Now do a merge where left_on and right_on have one column different.
    tb = tables.merge(table_1, table_2, left_on=["country", "year", "b"], right_on=["country", "year", "c"])
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.merge(table_2, left_on=["country", "year", "b"], right_on=["country", "year", "c"]))
    # Column "country" has the same title on both tables, but has description only on table_1, therefore when combining
    # with table_2, title should be preserved, but description will be lost.
    assert tb["country"].metadata.title == "Country Title"
    assert tb["country"].metadata.description is None
    # Idem for "year".
    assert tb["year"].metadata.title is None
    assert tb["year"].metadata.description is None
    # Given that there is a column "a" in both left and right tables, they will be renamed as "a_x" and "a_y" in the
    # merged table. Then, "a_x" should have the metadata of the left table, and "a_y" should have the metadata of the
    # right table.
    assert tb["a_x"].metadata == table_1["a"].metadata
    assert tb["a_y"].metadata == table_2["a"].metadata
    # Column "b" appears only in table_1, so it should keep its original metadata.
    assert tb["b"].metadata == table_1["b"].metadata
    # Column "c" appears only in table_2, so it should keep its original metadata.
    assert tb["c"].metadata == table_2["c"].metadata
    # Now check the table metadata.
    # Since titles and descriptions of the tables concatenated are different, title and description should be empty.
    assert tb.metadata.title is None
    assert tb.metadata.description is None
    # Sources and licenses should be all sources and licenses of the resulting variables.
    # assert tb.metadata.dataset.sources == [sources[2], sources[1], sources[3], sources[4]]  # type: ignore
    # assert tb.metadata.dataset.licenses == [licenses[1], licenses[2], licenses[3], licenses[4]]  # type: ignore

    # Now do a merge where column "a" is included both on left_on and right_on.
    tb = tables.merge(table_1, table_2, left_on=["country", "year", "a"], right_on=["country", "year", "a"])
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.merge(table_2, left_on=["country", "year", "a"], right_on=["country", "year", "a"]))
    # Column "country" has the same title on both tables, but has description only on table_1, therefore when combining
    # with table_2, title should be preserved, but description will be lost.
    assert tb["country"].metadata.title == "Country Title"
    assert tb["country"].metadata.description is None
    # Idem for "year".
    assert tb["year"].metadata.title is None
    assert tb["year"].metadata.description is None
    # Column "a" should now combine metadata from left and right tables.
    # Given that they have differring titles and description, the combined title and description should disappear.
    assert tb["a"].metadata.title is None
    assert tb["a"].metadata.description is None
    # Sources and licenses should be the combination from the two tables.
    assert tb["a"].metadata.sources == [sources[2], sources[1]]
    assert tb["a"].metadata.licenses == [licenses[1], licenses[2]]
    # Column "b" appears only in table_1, so it should keep its original metadata.
    assert tb["b"].metadata == table_1["b"].metadata
    # Column "c" appears only in table_2, so it should keep its original metadata.
    assert tb["c"].metadata == table_2["c"].metadata
    # Now check the table metadata.
    # Since titles and descriptions of the tables concatenated are different, title and description should be empty.
    assert tb.metadata.title is None
    assert tb.metadata.description is None
    # Sources and licenses should be all sources and licenses of the resulting variables.
    # assert tb.metadata.dataset.sources == [sources[2], sources[1], sources[3], sources[4]]  # type: ignore
    # assert tb.metadata.dataset.licenses == [licenses[1], licenses[2], licenses[3], licenses[4]]  # type: ignore


def test_concat_with_axis_0(table_1, table_2, sources, licenses) -> None:
    tb = tables.concat([table_1, table_2])
    # Column "country" has the same title on both tables, but has description only on table_1, therefore when combining
    # with table_2, title should be preserved, but description will be lost.
    assert tb["country"].metadata.title == "Country Title"
    assert tb["country"].metadata.description is None
    # Column "year" has no title and no description in any of the tables.
    assert tb["year"].metadata.title is None
    assert tb["year"].metadata.description is None
    # Column "a" appears in both tables, so the resulting "a" columns should combine sources and licenses.
    assert tb["a"].metadata.title is None
    assert tb["a"].metadata.description is None
    assert tb["a"].metadata.sources == [sources[2], sources[1]]
    assert tb["a"].metadata.licenses == [licenses[1], licenses[2]]
    # Column "b" appears only in table_1, so it should keep its original metadata.
    assert tb["b"].metadata == table_1["b"].metadata
    # Column "c" appears only in table_2, so it should keep its original metadata.
    assert tb["c"].metadata == table_2["c"].metadata
    # Now check the table metadata.
    # Since titles and descriptions of the tables concatenated are different, title and description should be empty.
    assert tb.metadata.title is None
    assert tb.metadata.description is None
    # Sources and licenses should be all sources and licenses of the resulting variables.
    # assert tb.metadata.dataset.sources == [sources[2], sources[1], sources[3], sources[4]]  # type: ignore
    # assert tb.metadata.dataset.licenses == [licenses[1], licenses[2], licenses[3], licenses[4]]  # type: ignore


def test_concat_with_axis_1(table_1, table_2, sources, licenses) -> None:
    # TODO: Assert that concat raises an error if the resulting table has multiple columns with the same name.
    # tb = tables.concat([table_1, table_2], axis=1)
    # Rename columns in table_2 so that they don't coincide with names in table_1.
    tb = tables.concat(
        [table_1, table_2.rename(columns={"country": "country_right", "year": "year_right", "a": "a_right"})], axis=1
    )
    # Argument axis=1 implies that columns are added in parallel, so they should all keep their original metadata.
    assert tb["country"].metadata == table_1["country"].metadata
    assert tb["year"].metadata == table_1["year"].metadata
    assert tb["a"].metadata == table_1["a"].metadata
    assert tb["b"].metadata == table_1["b"].metadata
    assert tb["country_right"].metadata == table_2["country"].metadata
    assert tb["year_right"].metadata == table_2["year"].metadata
    assert tb["a_right"].metadata == table_2["a"].metadata
    assert tb["c"].metadata == table_2["c"].metadata
    # Now check the table metadata.
    # Since titles and descriptions of the tables concatenated are different, title and description should be empty.
    assert tb.metadata.title is None
    assert tb.metadata.description is None
    # Sources and licenses should be all sources and licenses of the resulting variables.
    # assert tb.metadata.dataset.sources == [sources[2], sources[1], sources[3], sources[4]]  # type: ignore
    # assert tb.metadata.dataset.licenses == [licenses[1], licenses[2], licenses[3], licenses[4]]  # type: ignore


def test_melt(table_1, sources, licenses) -> None:
    # If nothing specified, all columns are melted.
    tb = tables.melt(table_1)
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.melt())
    for column in ["variable", "value"]:
        # Given that titles and descriptions are different, they should not be propagated.
        assert tb[column].metadata.title is None
        assert tb[column].metadata.description is None
        # Sources and licenses should be the combination of sources and licenses of all columns in table_1.
        assert tb[column].metadata.sources == [sources[2], sources[1], sources[3]]
        assert tb[column].metadata.licenses == [licenses[1], licenses[2], licenses[3]]
    # Now check that table metadata is identical.
    assert tb.metadata == table_1.metadata

    # Repeat the same operation, but specifying different names for variable and value columns.
    tb = tables.melt(table_1, var_name="var", value_name="val")
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.melt(var_name="var", value_name="val"))
    for column in ["var", "val"]:
        # Given that titles and descriptions are different, they should not be propagated.
        assert tb[column].metadata.title is None
        assert tb[column].metadata.description is None
        # Sources and licenses should be the combination of sources and licenses of all columns in table_1.
        assert tb[column].metadata.sources == [sources[2], sources[1], sources[3]]
        assert tb[column].metadata.licenses == [licenses[1], licenses[2], licenses[3]]
    # Now check that table metadata is identical.
    assert tb.metadata == table_1.metadata

    # Specify fixed columns; all other columns will be melted.
    tb = tables.melt(table_1, id_vars=["country", "year"])
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.melt(id_vars=["country", "year"]))
    assert tb["country"].metadata.title == table_1["country"].metadata.title
    assert tb["country"].metadata.description == table_1["country"].metadata.description
    assert tb["year"].metadata.title is None
    assert tb["year"].metadata.description is None
    # The new "variable" and "value" columns should combine the sources and licenses of "a" and "b".
    for column in ["variable", "value"]:
        assert tb[column].metadata.sources == [sources[2], sources[1], sources[3]]
        assert tb[column].metadata.licenses == [licenses[1], licenses[2], licenses[3]]
    # Now check that table metadata is identical.
    assert tb.metadata == table_1.metadata

    # Repeat the same operation, but specifying different names for variable and value columns.
    # Specify fixed columns; all other columns will be melted.
    tb = tables.melt(table_1, id_vars=["country", "year"], var_name="var", value_name="val")
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.melt(id_vars=["country", "year"], var_name="var", value_name="val"))
    assert tb["country"].metadata.title == table_1["country"].metadata.title
    assert tb["country"].metadata.description == table_1["country"].metadata.description
    assert tb["year"].metadata.title is None
    assert tb["year"].metadata.description is None
    # The new "variable" and "value" columns should combine the sources and licenses of "a" and "b".
    for column in ["var", "val"]:
        assert tb[column].metadata.sources == [sources[2], sources[1], sources[3]]
        assert tb[column].metadata.licenses == [licenses[1], licenses[2], licenses[3]]
    # Now check that table metadata is identical.
    assert tb.metadata == table_1.metadata

    # Specify fixed columns and variable columns.
    # Specify fixed columns; all other columns will be melted.
    tb = tables.melt(table_1, id_vars=["country", "year"], value_vars=["b"])
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.melt(id_vars=["country", "year"], value_vars=["b"]))
    assert tb["country"].metadata.title == table_1["country"].metadata.title
    assert tb["country"].metadata.description == table_1["country"].metadata.description
    assert tb["year"].metadata.title is None
    assert tb["year"].metadata.description is None
    # The new "variable" and "value" columns should have the same metadata as the original "b".
    for column in ["variable", "value"]:
        assert tb[column].metadata.title == table_1["b"].metadata.title
        assert tb[column].metadata.description == table_1["b"].metadata.description
        assert tb[column].metadata.sources == table_1["b"].metadata.sources
        assert tb[column].metadata.licenses == table_1["b"].metadata.licenses
    # Now check that table metadata is identical.
    assert tb.metadata == table_1.metadata


def test_pivot(table_1, sources) -> None:
    # To better test the expected behaviour, I will add a source to the "country" column.
    table_1["country"].metadata.sources = [sources[4]]
    tb = tables.pivot(table_1, columns="country")
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.pivot(columns="country"))
    # Column "a" with all its sublevels should keep the metadata of the original "a".
    # By construction, the metadata of "country" is currently lost when pivoting.
    # Alternatively we could combine the metadata of "a" and "country".
    # Note: Currently, tb[("a", "France")].metadata shows the original metadata, but tb["a"]["France"].metadata doesn't.
    assert tb[("a", "France")].metadata == table_1["a"].metadata
    assert tb[("a", "Spain")].metadata == table_1["a"].metadata
    assert tb[("b", "France")].metadata == table_1["b"].metadata
    assert tb[("b", "Spain")].metadata == table_1["b"].metadata
    # Now check that table metadata is identical.
    assert tb.metadata == table_1.metadata

    # Add a new column to pivot.
    tb = tables.pivot(table_1, columns=["country", "year"])
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.pivot(columns=["country", "year"]))
    assert tb[("a", "Spain", 2020)].metadata == table_1["a"].metadata
    # Now check that table metadata is identical.
    assert tb.metadata == table_1.metadata

    # Choose one of the columns as index.
    tb = tables.pivot(table_1, index="country", columns=["year"])
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.pivot(index="country", columns=["year"]))
    assert tb[("a", 2020)].metadata == table_1["a"].metadata
    # Now check that table metadata is identical.
    assert tb.metadata == table_1.metadata

    # Specify argument "value" as a column name.
    # When doing so, the new columns will be 2020 and 2021 (without the name "a" at any level).
    tb = tables.pivot(table_1, index="country", columns=["year"], values="a")
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.pivot(index="country", columns=["year"], values="a"))
    assert tb[2020].metadata == table_1["a"].metadata
    assert tb[2021].metadata == table_1["a"].metadata
    # Now check that table metadata is identical.
    assert tb.metadata == table_1.metadata

    # Specify argument "value" as a list.
    # When doing so, the new columns will have two levels, one for "a" and another for 2020 and 2021.
    tb = tables.pivot(table_1, index="country", columns=["year"], values=["a"])
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.pivot(index="country", columns=["year"], values=["a"]))
    assert tb[("a", 2020)].metadata == table_1["a"].metadata
    assert tb[("a", 2021)].metadata == table_1["a"].metadata
    # Now check that table metadata is identical.
    assert tb.metadata == table_1.metadata

    # Specify argument "value" as a list and flatten levels.
    tb = tables.pivot(table_1, index="country", columns=["year"], values=["a"], join_column_levels_with="_")
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.pivot(index="country", columns=["year"], values=["a"], join_column_levels_with="_"))
    assert tb["a_2020"].metadata == table_1["a"].metadata
    assert tb["a_2021"].metadata == table_1["a"].metadata
    assert tb["country"].metadata == table_1["country"].metadata
    # Now check that table metadata is identical.
    assert tb.metadata == table_1.metadata

    # Specify argument "value" as a column name and attempt to flatten levels.
    # The flattening is actually irrelevant, given that "value" is passed as a string and "a" is therefore not a level.
    tb = tables.pivot(table_1, index="country", columns=["year"], values="a", join_column_levels_with="_")
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.pivot(index="country", columns=["year"], values="a", join_column_levels_with="_"))
    assert tb[2020].metadata == table_1["a"].metadata
    assert tb[2021].metadata == table_1["a"].metadata
    assert tb["country"].metadata == table_1["country"].metadata
    # Now check that table metadata is identical.
    assert tb.metadata == table_1.metadata


def test_get_unique_sources_from_tables(table_1, sources):
    unique_sources = get_unique_sources_from_tables([table_1, table_1])
    assert unique_sources == [
        sources[2],
        sources[1],
        sources[3],
    ]

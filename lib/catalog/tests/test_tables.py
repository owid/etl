#
#  test_tables.py
#

import json
import tempfile
from collections import defaultdict
from os.path import exists, join, splitext

import jsonschema
import numpy as np
import pandas as pd
import pytest

from owid.catalog import VariablePresentationMeta, tables
from owid.catalog.datasets import FileFormat
from owid.catalog.meta import TableMeta, VariableMeta
from owid.catalog.tables import (
    SCHEMA,
    Table,
    get_unique_licenses_from_tables,
    get_unique_sources_from_tables,
    keep_metadata,
)
from owid.catalog.variables import Variable

from .mocking import mock


def test_create():
    t = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})
    assert list(t.gdp) == [100, 102, 104]
    assert list(t.country) == ["AU", "SE", "CH"]


def test_create_with_underscore():
    t = Table({"GDP": [100, 102, 104]}, underscore=True, short_name="GDP Table")
    assert t.columns == ["gdp"]
    assert t.metadata.short_name == "gdp_table"


def test_create_format():
    t = Table({"GDP": [100, 104, 102], "country": ["C", "A", "B"]}, short_name="GDP Table")
    t = t.format("country")
    ## Check underscore
    assert t.columns == ["gdp"]
    assert t.metadata.short_name == "gdp_table"
    ## Check index
    assert t.index.names == ["country"]
    ## Check sorting
    assert (t.index == ["A", "B", "C"]).all()

    # Check with default keys
    t = Table({"GDP": [100, 104, 102], "country": ["A", "A", "B"], "year": [2001, 2000, 2000]}, short_name="GDP Table")
    t = t.format()
    ## Check underscore
    assert t.columns == ["gdp"]
    assert t.metadata.short_name == "gdp_table"
    ## Check index
    assert t.index.names == ["country", "year"]
    ## Check sorting
    index_check = pd.MultiIndex.from_tuples([("A", 2000), ("A", 2001), ("B", 2000)], names=["country", "year"])
    assert index_check.equals(t.index)

    # Check error
    with pytest.raises(ValueError):
        t = Table({"GDP": [100, 104, 102], "country": ["A", "A", "B"]}, short_name="GDP Table")
        t = t.format("country")


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
    t: Table = Table({"gdp": [100, 100, 102, 104], "country": ["AU", "AU", "SE", "CH"]}).set_index("country")  # type: ignore
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
    t: Table = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})

    t.metadata = mock(TableMeta)
    t.metadata.primary_key = ["country"]
    for col in t.all_columns:
        t._fields[col] = mock(VariableMeta)
        if col == "country":
            t._fields[col].title = "country"

    t = t.set_index("country")  # type: ignore

    return t


def test_load_csv_table_over_http() -> None:
    Table.read_csv("https://catalog.ourworldindata.org/reference/countries_regions.csv")


def test_rename_columns() -> None:
    t: Table = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]}).set_index("country")  # type: ignore
    t.gdp.metadata.title = "GDP"
    new_t = t.rename(columns={"gdp": "new_gdp"})
    assert new_t.new_gdp.metadata.title == "GDP"
    assert new_t.columns == ["new_gdp"]

    new_t.new_gdp.metadata.title = "New GDP"

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


def test_copy_metadata() -> None:
    t: Table = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})
    t.metadata.title = "GDP table"
    t.gdp.metadata.title = "GDP"

    t2: Table = Table(pd.DataFrame(t))
    t2.country.metadata.title = "Country"

    t2 = t2.copy_metadata(t)

    assert t2.gdp.metadata.title == "GDP"
    assert t2.country.metadata.title == "Country"
    assert t2.metadata.title == "GDP table"

    # make sure it doesn't affect the original table
    t2.gdp.metadata.title = "new GDP"
    assert t.gdp.metadata.title == "GDP"


def test_addition_same_variable() -> None:
    t: Table = Table({"a": [1, 2], "b": [3, 4]})
    t.a.metadata.title = "A"
    t.b.metadata.title = "B"

    t["a"] = t["a"] + t["b"]

    # Now variable "a" has a different meaning, so the title should not be preserved (but "b"'s title should).
    assert t.a.metadata.title is None
    assert t.b.metadata.title == "B"

    # However, if "b" did not have a title, when adding both, the title of "a" should be preserved.
    t: Table = Table({"a": [1, 2], "b": [3, 4]})
    t.a.metadata.title = "A"
    t.b.metadata.title = None

    t["a"] = t["a"] + t["b"]
    assert t.a.metadata.title == "A"
    assert t.b.metadata.title is None


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

    # dimension has been added to metadata
    assert tb_new.m.dimensions
    assert len(tb_new.m.dimensions) == 1
    assert tb_new.m.dimensions[0] == {"name": "A", "slug": "a"}

    tb_new = tb_new.reset_index()

    # dimension has been dropped
    assert not tb_new.m.dimensions

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


def test_merge_without_any_on_arguments(table_1, table_2, sources, origins, licenses) -> None:
    # If "on", "left_on" and "right_on" are not specified, the join is performed on common columns.
    # In this case, "country", "year", "a".
    tb = tables.merge(table_1, table_2)
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.merge(table_2))
    # Check that non-overlapping columns preserve metadata.
    assert tb["c"].metadata == table_2["c"].metadata
    # Check that "on" columns combine the metadata of left and right tables.
    # Column "country" has the same title on both tables, and has description only on table_1, therefore when combining
    # with table_2, the unique title should be preserved, and the only existing description should persist.
    assert tb["country"].metadata.title == "Country Title"
    assert tb["country"].metadata.description == "Description of Table 1 Variable country"
    # Column "year" has no metadata in either table.
    assert tb["year"].metadata.title is None
    assert tb["year"].metadata.description is None
    # Column "a" should combine the sources and licenses of of both tables (not title and description, since they are
    # different in left and right tables).
    assert tb["a"].metadata.title is None
    assert tb["a"].metadata.description is None
    assert tb["a"].metadata.sources == [sources[2], sources[1]]
    assert tb["a"].metadata.origins == [origins[2], origins[1]]
    assert tb["a"].metadata.licenses == [licenses[1], licenses[2]]
    # Since table_1["a"] has processing level "minor" and table_2["a"] has "major", the combination should be "major".
    assert tb["a"].metadata.processing_level == "major"
    # Since table_1["a"] and table_2["a"] have identical presentation, the combination should have the same.
    assert tb["a"].metadata.presentation == table_1["a"].metadata.presentation
    # Since table_1["a"] and table_2["a"] have different display, the combination should have no display.
    assert tb["a"].metadata.display is None
    # Column "b" appears only in table_1, so it should keep its original metadata.
    assert tb["b"].metadata == table_1["b"].metadata
    # Column "c" appears only in table_2, so it should keep its original metadata.
    assert tb["c"].metadata == table_2["c"].metadata
    # Now check the table metadata.
    # Since titles and descriptions of the tables concatenated are different, title and description should be empty.
    assert tb.metadata.title is None
    assert tb.metadata.description is None


def test_merge_tables_where_only_one_has_title_or_description(table_1, table_2) -> None:
    tb1 = table_1.copy()
    tb2 = table_2.copy()
    # Delete title from tb1, and make description of tb2 identical to the description of tb1.
    tb1.metadata.title = None
    tb2.metadata.description = "Description of Table 1"
    tb = tables.merge(tb1, tb2)
    # The resulting table should have the title of tb2 (since tb1 has no title), and the description of tb1
    # (which is identical to the description of tb2).
    assert tb.metadata.title == "Title of Table 2"
    assert tb.metadata.description == "Description of Table 1"


def test_merge_with_on_argument(table_1, table_2) -> None:
    tb = tables.merge(table_1, table_2, on=["country", "year"])
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.merge(table_2, on=["country", "year"]))
    # Column "country" has the same title on both tables, and has description only on table_1, therefore when combining
    # with table_2, the unique title should be preserved, and the only existing description should persist.
    assert tb["country"].metadata.title == "Country Title"
    assert tb["country"].metadata.description == "Description of Table 1 Variable country"
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


def test_merge_with_left_on_and_right_on_argument(table_1, table_2, sources, origins, licenses) -> None:
    # Join on columns "country" and "year" (in this case, the result should be identical to simply defining
    # on=["country", "year"]).
    tb = tables.merge(table_1, table_2, left_on=["country", "year"], right_on=["country", "year"])
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.merge(table_2, left_on=["country", "year"], right_on=["country", "year"]))
    # Column "country" has the same title on both tables, and has description only on table_1, therefore when combining
    # with table_2, the unique title should be preserved, and the only existing description should persist.
    assert tb["country"].metadata.title == "Country Title"
    assert tb["country"].metadata.description == "Description of Table 1 Variable country"
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

    # Now do a merge where left_on and right_on have one column different.
    tb = tables.merge(table_1, table_2, left_on=["country", "year", "b"], right_on=["country", "year", "c"])
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.merge(table_2, left_on=["country", "year", "b"], right_on=["country", "year", "c"]))
    # Column "country" has the same title on both tables, and has description only on table_1, therefore when combining
    # with table_2, the unique title should be preserved, and the only existing description should persist.
    assert tb["country"].metadata.title == "Country Title"
    assert tb["country"].metadata.description == "Description of Table 1 Variable country"
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

    # Now do a merge where column "a" is included both on left_on and right_on.
    tb = tables.merge(table_1, table_2, left_on=["country", "year", "a"], right_on=["country", "year", "a"])
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.merge(table_2, left_on=["country", "year", "a"], right_on=["country", "year", "a"]))
    # Column "country" has the same title on both tables, and has description only on table_1, therefore when combining
    # with table_2, the unique title should be preserved, and the only existing description should persist.
    assert tb["country"].metadata.title == "Country Title"
    assert tb["country"].metadata.description == "Description of Table 1 Variable country"
    # Idem for "year".
    assert tb["year"].metadata.title is None
    assert tb["year"].metadata.description is None
    # Column "a" should now combine metadata from left and right tables.
    # Given that they have differing titles and description, the combined title and description should disappear.
    assert tb["a"].metadata.title is None
    assert tb["a"].metadata.description is None
    # Sources, origins and licenses should be the combination from the two tables.
    assert tb["a"].metadata.sources == [sources[2], sources[1]]
    assert tb["a"].metadata.origins == [origins[2], origins[1]]
    assert tb["a"].metadata.licenses == [licenses[1], licenses[2]]
    # Column "b" appears only in table_1, so it should keep its original metadata.
    assert tb["b"].metadata == table_1["b"].metadata
    # Column "c" appears only in table_2, so it should keep its original metadata.
    assert tb["c"].metadata == table_2["c"].metadata
    # Now check the table metadata.
    # Since titles and descriptions of the tables concatenated are different, title and description should be empty.
    assert tb.metadata.title is None
    assert tb.metadata.description is None


def test_merge_keeps_metadata(table_1, table_2, origins) -> None:
    table_1.a.m.origins = [origins[1]]
    _ = tables.merge(table_1, table_2, on=["country", "year"])
    assert table_1.a.m.origins == [origins[1]]


def test_merge_categoricals(table_1, table_2, origins) -> None:
    table_1.country.m.origins = [origins[1]]
    table_2.loc[0, "country"] = "Poland"

    # both tables have different categories for "country"
    table_1 = table_1.astype({"country": "category"})
    table_2 = table_2.astype({"country": "category"})

    tb = tables.merge(table_1, table_2, on=["country", "year"])

    # categorical type and metadata are preserved despite having different categories
    assert tb.country.dtype == "category"
    assert tb.country.m.origins[0] == origins[1]


def test_merge_with_dimensions(table_1, table_2) -> None:
    table_1.a.m.dimensions = {"sex": "male"}
    table_2.c.m.dimensions = {"sex": "female"}
    tb = tables.merge(table_1[["country", "year", "a"]], table_2[["country", "year", "c"]], on=["country", "year"])

    assert tb.a.m.dimensions == {"sex": "male"}
    assert tb.c.m.dimensions == {"sex": "female"}


def test_concat_categoricals(table_1, table_2, origins) -> None:
    table_1.country.m.origins = [origins[1]]
    table_2.loc[0, "country"] = "Poland"

    # both tables have different categories for "country"
    table_1 = table_1.astype({"country": "category"})
    table_2 = table_2.astype({"country": "category"})

    tb = tables.concat([table_1, table_2])

    # categorical type and metadata are preserved despite having different categories
    assert tb.country.dtype == "category"
    assert tb.country.m.origins[0] == origins[1]


def test_concat_with_axis_0(table_1, table_2, sources, origins, licenses) -> None:
    tb = tables.concat([table_1, table_2])
    # Column "country" has the same title on both tables, and has description only on table_1, therefore when combining
    # with table_2, the unique title should be preserved, and the only existing description should persist.
    assert tb["country"].metadata.title == "Country Title"
    assert tb["country"].metadata.description == "Description of Table 1 Variable country"
    # Column "year" has no title and no description in any of the tables.
    assert tb["year"].metadata.title is None
    assert tb["year"].metadata.description is None
    # Column "a" appears in both tables, so the resulting "a" columns should combine sources and licenses.
    assert tb["a"].metadata.title is None
    assert tb["a"].metadata.description is None
    assert tb["a"].metadata.sources == [sources[2], sources[1]]
    assert tb["a"].metadata.origins == [origins[2], origins[1]]
    assert tb["a"].metadata.licenses == [licenses[1], licenses[2]]
    # Column "b" appears only in table_1, so it should keep its original metadata.
    assert tb["b"].metadata == table_1["b"].metadata
    # Column "c" appears only in table_2, so it should keep its original metadata.
    assert tb["c"].metadata == table_2["c"].metadata
    # Now check the table metadata.
    # Since titles and descriptions of the tables concatenated are different, title and description should be empty.
    assert tb.metadata.title is None
    assert tb.metadata.description is None


def test_concat_with_axis_1(table_1, table_2) -> None:
    # TODO: Assert that concat raises an error if the resulting table has multiple columns with the same name.
    # tb = tables.concat([table_1, table_2], axis=1)

    # Concat along axis 1 should preserve all metadata, even display.
    table_1.a.metadata.display = {"unit": "foo"}

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


def test_melt(table_1, sources, origins, licenses) -> None:
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
        assert tb[column].metadata.origins == [origins[2], origins[1], origins[3]]
        assert tb[column].metadata.licenses == [licenses[1], licenses[2], licenses[3]]
        # The combination should have the largest processing level of both variables combined.
        assert tb[column].metadata.processing_level == "major"
        # Since "a" and "b" have different presentation, the combination should have no presentation.
        assert tb[column].metadata.presentation is None
        # Since "a" and "b" have identical display, the combination should have the same display.
        assert tb[column].metadata.display == table_1["a"].metadata.display
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
        assert tb[column].metadata.origins == [origins[2], origins[1], origins[3]]
        assert tb[column].metadata.licenses == [licenses[1], licenses[2], licenses[3]]
        # The combination should have the largest processing level of both variables combined.
        assert tb[column].metadata.processing_level == "major"
        # Since "a" and "b" have different presentation, the combination should have no presentation.
        assert tb[column].metadata.presentation is None
        # Since "a" and "b" have identical display, the combination should have the same display.
        assert tb[column].metadata.display == table_1["a"].metadata.display
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
        assert tb[column].metadata.origins == [origins[2], origins[1], origins[3]]
        assert tb[column].metadata.licenses == [licenses[1], licenses[2], licenses[3]]
        # The combination should have the largest processing level of both variables combined.
        assert tb[column].metadata.processing_level == "major"
        # Since "a" and "b" have different presentation, the combination should have no presentation.
        assert tb[column].metadata.presentation is None
        # Since "a" and "b" have identical display, the combination should have the same display.
        assert tb[column].metadata.display == table_1["a"].metadata.display
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
        assert tb[column].metadata.origins == [origins[2], origins[1], origins[3]]
        assert tb[column].metadata.licenses == [licenses[1], licenses[2], licenses[3]]
        # The combination should have the largest processing level of both variables combined.
        assert tb[column].metadata.processing_level == "major"
        # Since "a" and "b" have different presentation, the combination should have no presentation.
        assert tb[column].metadata.presentation is None
        # Since "a" and "b" have identical display, the combination should have the same display.
        assert tb[column].metadata.display == table_1["a"].metadata.display
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
        assert tb[column].metadata.origins == table_1["b"].metadata.origins
        assert tb[column].metadata.licenses == table_1["b"].metadata.licenses
        assert tb[column].metadata.processing_level == table_1["b"].metadata.processing_level
        assert tb[column].metadata.presentation == table_1["b"].metadata.presentation
        assert tb[column].metadata.display == table_1["b"].metadata.display
    # Now check that table metadata is identical.
    assert tb.metadata == table_1.metadata


def test_pivot(table_1, sources, origins) -> None:
    # To better test the expected behavior, I will add a source (and origin) to the "country" column.
    table_1["country"].metadata.sources = [sources[4]]
    table_1["country"].metadata.origins = [origins[4]]
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
    # Remove dimensions to make them equal
    tb[("a", "Spain", 2020)].m.dimensions = None
    assert tb[("a", "Spain", 2020)].metadata == table_1["a"].metadata
    # Now check that table metadata is identical.
    assert tb.metadata == table_1.metadata

    # Choose one of the columns as index.
    tb = tables.pivot(table_1, index="country", columns=["year"])
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.pivot(index="country", columns=["year"]))
    # Remove dimensions to make them equal
    tb[("a", 2020)].m.dimensions = None
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
    # Remove dimensions to make them equal
    tb[("a", 2020)].m.dimensions = None
    tb[("a", 2021)].m.dimensions = None
    assert tb[("a", 2020)].metadata == table_1["a"].metadata
    assert tb[("a", 2021)].metadata == table_1["a"].metadata
    # Now check that table metadata is identical.
    assert tb.metadata == table_1.metadata

    # Specify argument "value" as a list and flatten levels.
    tb = tables.pivot(table_1, index="country", columns=["year"], values=["a"], join_column_levels_with="_")
    # Check that the result is identical to using the table method.
    assert tb.equals_table(table_1.pivot(index="country", columns=["year"], values=["a"], join_column_levels_with="_"))
    # Remove dimensions to make them equal
    for col in ["a_2020", "a_2021"]:
        tb[col].m.dimensions = None
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


def test_pivot_metadata_propagation():
    tb = Table(
        pd.DataFrame(
            {
                "year": [2020, 2021, 2022],
                "group": ["g1", "g1", "g1"],
                "subgroup": ["s1", "s1", "s2"],
                "value": [1, 2, 3],
            }
        )
    )
    tb["value"].m.presentation = VariablePresentationMeta(title_public="Value")
    tb_p = tb.pivot(index="year", columns=["group", "subgroup"], values="value", join_column_levels_with="_")

    # Set title_public for one of the variables
    tb_p["g1_s1"].m.presentation.title_public = "Group 1, Subgroup 1"

    # It should not affect the other variable
    assert tb_p["g1_s2"].m.presentation.title_public == "Value"


def test_pivot_dimensions():
    tb = Table(
        pd.DataFrame(
            {
                "year": [2020, 2021, 2022],
                "group": ["g1", "g1", "g1"],
                "subgroup": ["s1", "s1", "s2"],
                "value": [1, 2, 3],
            }
        )
    )
    tb["value"].m.presentation = VariablePresentationMeta(title_public="Value")
    tb_p = tb.pivot(index="year", columns=["group", "subgroup"], values="value", join_column_levels_with="_")

    assert tb_p.g1_s1.m.dimensions == {"group": "g1", "subgroup": "s1"}
    assert tb_p.g1_s2.m.dimensions == {"group": "g1", "subgroup": "s2"}


def test_get_unique_sources_from_tables(table_1, sources):
    unique_sources = get_unique_sources_from_tables([table_1, table_1])
    assert unique_sources == [
        sources[2],
        sources[1],
        sources[3],
    ]


def test_get_unique_license_from_tables(table_1, licenses):
    unique_licenses = get_unique_licenses_from_tables([table_1, table_1])
    assert unique_licenses == [
        licenses[1],
        licenses[2],
        licenses[3],
    ]


def test_sum_columns(table_1, sources, origins, licenses):
    # Create a new column that is the element-wise sum of the other two existing columns.
    table_1["c"] = table_1[["a", "b"]].sum(axis=1)
    assert table_1["c"].metadata.sources == [sources[2], sources[1], sources[3]]
    assert table_1["c"].metadata.origins == [origins[2], origins[1], origins[3]]
    assert table_1["c"].metadata.licenses == [licenses[1], licenses[2], licenses[3]]
    assert table_1["c"].metadata.title is None
    assert table_1["c"].metadata.description is None
    assert table_1["c"].metadata.processing_level == "major"
    assert table_1["c"].metadata.presentation is None
    assert table_1["c"].metadata.display == table_1["a"].metadata.display

    # Create a new variable (it cannot be added as a new column since it has different dimensions) that is the sum of
    # each of the other two existing columns.
    variable_c = table_1[["a", "b"]].sum(axis=0)
    assert variable_c.metadata.sources == [sources[2], sources[1], sources[3]]
    assert variable_c.metadata.origins == [origins[2], origins[1], origins[3]]
    assert variable_c.metadata.licenses == [licenses[1], licenses[2], licenses[3]]
    assert variable_c.metadata.title is None
    assert variable_c.metadata.description is None
    assert variable_c.metadata.processing_level == "major"
    assert variable_c.metadata.presentation is None
    assert variable_c.metadata.display == table_1["a"].metadata.display


def test_operations_of_table_and_scalar(table_1, sources, origins, licenses):
    table_1_original = table_1.copy()
    table_1[["a", "b"]] = table_1[["a", "b"]] + 1
    table_1[["a", "b"]] += 1
    table_1[["a", "b"]] = table_1[["a", "b"]] - 1
    table_1[["a", "b"]] -= 1
    table_1[["a", "b"]] = table_1[["a", "b"]] * 1
    table_1[["a", "b"]] *= 1
    table_1[["a", "b"]] = table_1[["a", "b"]] / 1
    table_1[["a", "b"]] /= 1
    table_1[["a", "b"]] = table_1[["a", "b"]] // 1
    table_1[["a", "b"]] //= 1
    table_1[["a", "b"]] = table_1[["a", "b"]] % 1
    table_1[["a", "b"]] %= 1
    table_1[["a", "b"]] = table_1[["a", "b"]] ** 1
    table_1[["a", "b"]] **= 1

    # Check that the metadata of both variables is preserved (only the processing log should have changed).
    assert table_1["a"].metadata == table_1_original["a"].metadata
    assert table_1["b"].metadata == table_1_original["b"].metadata


def test_multiply_columns(table_1, sources, origins, licenses):
    # Create a new column that is the element-wise product of the other two existing columns.
    table_1["c"] = table_1[["a", "b"]].prod(axis=1)
    assert table_1["c"].metadata.sources == [sources[2], sources[1], sources[3]]
    assert table_1["c"].metadata.origins == [origins[2], origins[1], origins[3]]
    assert table_1["c"].metadata.licenses == [licenses[1], licenses[2], licenses[3]]
    assert table_1["c"].metadata.title is None
    assert table_1["c"].metadata.description is None
    assert table_1["c"].metadata.processing_level == "major"
    assert table_1["c"].metadata.presentation is None
    assert table_1["c"].metadata.display == table_1["a"].metadata.display

    # Create a new variable (it cannot be added as a new column since it has different dimensions) that is the product
    # of each of the other two existing columns.
    variable_c = table_1[["a", "b"]].prod(axis=0)
    assert variable_c.metadata.sources == [sources[2], sources[1], sources[3]]
    assert variable_c.metadata.origins == [origins[2], origins[1], origins[3]]
    assert variable_c.metadata.licenses == [licenses[1], licenses[2], licenses[3]]
    assert variable_c.metadata.title is None
    assert variable_c.metadata.description is None
    assert variable_c.metadata.processing_level == "major"
    assert variable_c.metadata.presentation is None
    assert variable_c.metadata.display == table_1["a"].metadata.display


def test_groupby_sum(table_1) -> None:
    gt = table_1.groupby("country").a.sum()
    assert gt.values.tolist() == [3, 3]
    assert gt.m.title == "Title of Table 1 Variable a"

    gt = table_1.groupby("country")["a"].sum()
    assert gt.values.tolist() == [3, 3]
    assert gt.m.title == "Title of Table 1 Variable a"

    gt = table_1.groupby("country")[["a", "b"]].sum()
    assert gt.values.tolist() == [[3, 6], [3, 9]]
    assert gt.a.m.title == "Title of Table 1 Variable a"
    assert gt.b.m.title == "Title of Table 1 Variable b"


def test_groupby_agg(table_1) -> None:
    gt = table_1.groupby("country")[["a", "b"]].agg("sum")
    assert gt.values.tolist() == [[3, 6], [3, 9]]
    assert gt["a"].m.title == "Title of Table 1 Variable a"

    gt = table_1.groupby("country").a.agg(["min", "max"])
    assert gt.values.tolist() == [[3, 3], [1, 2]]
    assert gt["min"].m.title == "Title of Table 1 Variable a"

    gt = table_1.groupby("country").a.agg("min")
    assert gt.values.tolist() == [3, 1]
    assert gt.m.title == "Title of Table 1 Variable a"

    def has_nan(x: pd.Series) -> bool:
        """Check if there is a NaN in a group."""
        return x.isna().any()

    gt = table_1.groupby("country")[["a", "b"]].agg(
        {
            "a": [has_nan, sum],
            "b": sum,
        }
    )
    assert gt.columns.tolist() == [("a", "has_nan"), ("a", "sum"), ("b", "sum")]
    assert gt.values.tolist() == [[False, 3, 6], [False, 3, 9]]
    assert isinstance(gt.a, Table)

    gt = table_1.groupby("country").agg(
        min_a=("a", "min"),
    )
    assert gt.min_a.values.tolist() == [3, 1]
    assert gt.min_a.m.title == "Title of Table 1 Variable a"


def test_groupby_apply_table(table_1) -> None:
    def func(tb):
        return tb[["a"]].rename(columns={"a": "c"})

    tb = table_1.groupby("country").apply(func)
    assert tb.columns.tolist() == ["c"]
    assert tb.c.m.title == "Title of Table 1 Variable a"


def test_groupby_apply_variable(table_1) -> None:
    def func(tb):
        return tb["a"] + 1

    a_ser = pd.DataFrame(table_1).groupby("country").apply(func)
    a_var = table_1.groupby("country").apply(func)
    assert a_ser.equals(pd.Series(a_var))
    assert a_var.m.title == "Title of Table 1 Variable a"


def test_groupby_apply_variable_2(table_1) -> None:
    def func(tb):
        return Variable({"c": 1})

    df_out = pd.DataFrame(table_1).groupby("country", as_index=False).apply(func)
    tb_out = table_1.groupby("country", as_index=False).apply(func)
    assert df_out.equals(pd.DataFrame(tb_out))
    assert tb_out.country.m.title == "Country Title"


def test_groupby_apply_constant(table_1) -> None:
    def func(tb):
        return 1

    df_out = pd.DataFrame(table_1).groupby("country", as_index=False).apply(func)
    tb_out = table_1.groupby("country", as_index=False).apply(func)
    assert df_out.equals(pd.DataFrame(tb_out))
    assert tb_out.country.m.title == "Country Title"


def test_groupby_count(table_1) -> None:
    gt = table_1.groupby("country").count()
    assert gt.values.tolist() == [[1, 1, 1], [2, 2, 2]]
    assert gt.a.m.title == "Title of Table 1 Variable a"


def test_groupby_transform(table_1) -> None:
    # column named `count` should work
    gt = table_1.rename(columns={"a": "count"}).groupby("country")["count"].transform("sum")
    assert gt.values.tolist() == [3, 3, 3]
    assert gt.m.title == "Title of Table 1 Variable a"


def test_groupby_size(table_1) -> None:
    gt = table_1.groupby("country").size()
    assert gt.values.tolist() == [1, 2]
    assert gt.ndim == 1
    assert isinstance(gt, pd.Series)


def test_groupby_fillna(table_1) -> None:
    gt = table_1.groupby("country").a.ffill()
    assert gt.values.tolist() == [1, 2, 3]
    assert gt.m.title == "Title of Table 1 Variable a"
    # original title hasn't changed
    assert table_1.a.m.title == "Title of Table 1 Variable a"


def test_groupby_iteration(table_1) -> None:
    for _, group in table_1.groupby("country"):
        assert isinstance(group._fields, defaultdict)
        assert group.a.m.title == "Title of Table 1 Variable a"


def test_groupby_observed_default(table_1) -> None:
    table_1 = table_1.astype({"a": "category"}).query("a != 3")
    gt = table_1.groupby("a").min()
    assert len(gt) == 2


def test_groupby_levels(table_1) -> None:
    table_1 = table_1.set_index(["country", "year"])
    gt = table_1.groupby(level=[0, 1]).last()
    assert gt.values.tolist() == [[3, 6], [1, 4], [2, 5]]
    assert gt.a.m.title == "Title of Table 1 Variable a"


def test_groupby_as_index(table_1) -> None:
    table_1.m.title = "Table 1"
    table_1 = table_1.astype({"country": "category"})
    gt = table_1.groupby(["country", "year"], as_index=False)["a"].min()
    assert gt.m.primary_key == []
    assert gt.m.title == "Table 1"


def test_set_columns(table_1) -> None:
    table_1.columns = ["country", "year", "new_a", "new_b"]
    assert table_1.new_a.m.title == "Title of Table 1 Variable a"


def test_fillna_with_number(table_1) -> None:
    # Make a copy of table_1 and introduce a nan in it.
    table = table_1.copy()
    table.loc[0, "a"] = None
    # Now fill it up with a number.
    table["a"] = table["a"].fillna(0)
    # The metadata of "a" should be preserved.
    assert table["a"].metadata == table_1["a"].metadata


def test_fillna_with_another_variable(table_1, origins, licenses) -> None:
    # Make a copy of table_1 and introduce a nan in it.
    tb = table_1.copy()
    tb.loc[0, "a"] = None
    # Now, instead of filling the nan with a number, fill it with another variable from the same table.
    tb["a"] = tb["a"].fillna(tb["b"])
    # The origins of the resulting variable should combine the origins of "a" and "b".
    assert tb["a"].metadata.origins == [origins[2], origins[1], origins[3]]
    # Idem for licenses.
    assert tb["a"].metadata.licenses == [licenses[1], licenses[2], licenses[3]]
    # Column "country" should not be affected.
    assert tb["country"].metadata == table_1["country"].metadata
    # Column "year" should not be affected.
    assert tb["year"].metadata == table_1["year"].metadata
    # Columns "a" and "b" have different titles and descriptions, so the combination should have no title.
    assert tb["a"].metadata.title is None
    assert tb["a"].metadata.description is None
    # Column "b" should keep its original metadata.
    assert tb["b"].metadata == table_1["b"].metadata
    # Now check the table metadata has not changed.
    assert tb.metadata == table_1.metadata


def test_fillna_with_another_table(table_1, origins, licenses) -> None:
    # Make a copy of table_1 and introduce a nan in it.
    tb = table_1.copy()
    tb.loc[0, "a"] = None
    # Make another copy of table_1 with different origins and licenses.
    tb2 = table_1.copy()
    tb2.loc[0, "a"] = 10
    tb2["a"].metadata.description = "Some new description, different from table_1['a']"
    tb2["a"].metadata.origins = [origins[4]]
    tb2["a"].metadata.licenses = [licenses[4]]
    # Now, instead of filling the nan with a number, fill it with another variable from another table.
    tb = tb.fillna(tb2)
    # The origins of the resulting variable should combine the origins of table_1["a"] and tb2["a"].
    assert tb["a"].metadata.origins == [origins[2], origins[1], origins[4]]
    # Idem for licenses.
    assert tb["a"].metadata.licenses == [licenses[1], licenses[4]]
    # Column "country" should not be affected.
    assert tb["country"].metadata == table_1["country"].metadata
    assert tb2["country"].metadata == table_1["country"].metadata
    # Column "year" should not be affected.
    assert tb["year"].metadata == table_1["year"].metadata
    assert tb2["year"].metadata == table_1["year"].metadata
    # Columns tb["a"] and tb2["a"] have the same titles but different descriptions, so the combination should have the
    # same title but no description.
    assert tb["a"].metadata.title == table_1["a"].metadata.title
    assert tb["a"].metadata.description is None
    # # Now check the table metadata has not changed.
    assert tb.metadata == table_1.metadata
    assert tb2.metadata == table_1.metadata


def test_ffill_with_number(table_1) -> None:
    # Make a copy of table_1 and introduce a nan in it.
    table = table_1.copy()
    table.loc[1, "a"] = None
    # Now fill it up with a number.
    table["a"] = table["a"].ffill()
    # The metadata of "a" should be preserved.
    assert table["a"].metadata == table_1["a"].metadata
    assert table.loc[1, "a"] == table_1.loc[0, "a"]

    # Make a copy of table_1 and introduce a nan in it.
    table = table_1.copy()
    table.loc[1, "a"] = None
    # Now fill it up with a number.
    table["a"] = table["a"].ffill()
    # The metadata of "a" should be preserved.
    assert table["a"].metadata == table_1["a"].metadata
    assert table.loc[1, "a"] == table_1.loc[0, "a"]


def test_bfill_with_number(table_1) -> None:
    # Make a copy of table_1 and introduce a nan in it.
    table = table_1.copy()
    table.loc[0, "a"] = None
    # Now fill it up with a number.
    table["a"] = table["a"].bfill()
    # The metadata of "a" should be preserved.
    assert table["a"].metadata == table_1["a"].metadata
    assert table.loc[0, "a"] == table_1.loc[1, "a"]

    # Make a copy of table_1 and introduce a nan in it.
    table = table_1.copy()
    table.loc[0, "a"] = None
    # Now fill it up with a number.
    table["a"] = table["a"].bfill()
    # The metadata of "a" should be preserved.
    assert table["a"].metadata == table_1["a"].metadata
    assert table.loc[0, "a"] == table_1.loc[1, "a"]


def test_fillna_error(table_1: Table) -> None:
    with pytest.raises(ValueError):
        table_1["a"].fillna()


def test_keep_metadata_dataframe(table_1: Table) -> None:
    @keep_metadata
    def rolling_sum(df: pd.DataFrame) -> pd.DataFrame:
        return df.rolling(window=2, min_periods=1).sum()

    tb = rolling_sum(table_1[["a", "b"]])
    assert list(tb.a) == [1.0, 3.0, 5.0]
    assert tb.a.m.title == "Title of Table 1 Variable a"


def test_keep_metadata_series(table_1: Table) -> None:
    @keep_metadata
    def to_numeric(s: pd.Series) -> pd.Series:
        return pd.to_numeric(s)

    table_1.a = to_numeric(table_1.a)
    assert table_1.a.m.title == "Title of Table 1 Variable a"


def test_table_rolling(table_1: Table):
    tb = table_1[["a", "b"]].copy()

    rolling = tb.rolling(window=1).sum()
    assert rolling.a.m.title == table_1.a.m.title
    assert rolling.b.m.title == table_1.b.m.title

    # make sure we are not modifying the original table
    rolling.a.m.title = "new"
    assert table_1.a.m.title != "new"


def test_table_groupby_rolling(table_1: Table):
    tb = table_1.copy()

    rolling = tb.groupby("country").rolling(window=1).sum()
    assert rolling.a.m.title == table_1.a.m.title
    assert rolling.b.m.title == table_1.b.m.title

    # make sure we are not modifying the original table
    rolling.a.m.title = "new"
    assert table_1.a.m.title != "new"


def test_assign_table(table_1: Table):
    # simple assign of series
    tb = table_1[["a"]].copy()
    tb["b"] = table_1["b"]
    assert tb.b.m.title == "Title of Table 1 Variable b"

    # assign table
    tb = table_1[["a"]].copy()
    tb[["b"]] = table_1[["b"]]
    assert tb.b.m.title == "Title of Table 1 Variable b"

    # assign table to column, this is supported by pandas and should be by Table, too
    tb = table_1[["a"]].copy()
    tb["b"] = table_1[["b"]]
    assert tb.b.m.title == "Title of Table 1 Variable b"

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

from owid.catalog.datasets import FileFormat
from owid.catalog.meta import TableMeta, VariableMeta
from owid.catalog.tables import SCHEMA, Table
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
    assert m["fields"] == {"country": {}, "gdp": {}, "french_fries": {}}


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
    assert t.c.metadata == VariableMeta()


def test_addition_with_metadata() -> None:
    t: Table = Table({"a": [1, 2], "b": [3, 4]})
    t.a.metadata.title = "A"
    t.b.metadata.title = "B"

    t["c"] = t["a"] + t["b"]

    # addition should not inherit metadata
    assert t.c.metadata == VariableMeta()

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

    # addition shouldn't change the metadata of the original columns
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

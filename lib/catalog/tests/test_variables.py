#
#  test_variables
#

from collections import defaultdict

import pandas as pd
import pytest

from owid.catalog.meta import TableMeta, VariableMeta
from owid.catalog.tables import Table
from owid.catalog.variables import License, Source, Variable


def test_create_empty_variable() -> None:
    v = Variable(name="dog")
    assert v is not None


def test_create_unnamed_variable_fails() -> None:
    v = Variable()
    assert v.name is None

    # cannot access a metadata attribute without a name
    with pytest.raises(ValueError):
        v.title

    # cannot set a metadata attribute without a name
    with pytest.raises(ValueError):
        v.description = "Hello"


def test_create_non_empty_variable() -> None:
    v = Variable([1, 2, 3], name="dog")
    assert list(v) == [1, 2, 3]


def test_metadata_survives_slicing() -> None:
    v = Variable([1, 2, 3], name="dog")
    v.description = "Something amazing"

    assert isinstance(v.iloc[:2], Variable)
    # assert v.iloc[:2].description == v.description


def test_metadata_accessed_in_bulk() -> None:
    v = Variable([1, 2, 3], name="dog")
    assert v.metadata == VariableMeta()


def test_variable_can_be_type_cast() -> None:
    v = Variable([1, 2, 3], name="dog", dtype="object")
    v.metadata.description = "Blah blah..."
    v2 = v.astype("int")
    assert v2.name == v.name
    assert v2.metadata == v.metadata
    assert (v == v2).all()


@pytest.fixture
def sources():
    sources = {
        1: Source(name="Name of Source 1", description="Description of Source 1"),
        2: Source(name="Name of Source 2", description="Description of Source 2"),
        3: Source(name="Name of Source 3", description="Description of Source 3"),
        4: Source(name="Name of Source 4", description="Description of Source 4"),
    }
    return sources


@pytest.fixture
def licenses():
    licenses = {
        1: License(name="Name of License 1", url="URL of License 1"),
        2: License(name="Name of License 2", url="URL of License 2"),
        3: License(name="Name of License 3", url="URL of License 3"),
        4: License(name="Name of License 4", url="URL of License 4"),
    }
    return licenses


@pytest.fixture
def table_1(sources, licenses):
    tb1 = Table({"country": ["Spain", "Spain", "France"], "year": [2020, 2021, 2021], "a": [1, 2, 3], "b": [4, 5, 6]})
    tb1.metadata = TableMeta(title="Title of Table 1", description="Description of Table 1")
    tb1._fields = defaultdict(
        VariableMeta,
        {
            "a": VariableMeta(
                title="Title of Table 1 Variable a",
                description="Description of Table 1 Variable a",
                sources=[sources[2], sources[1]],
                licenses=[licenses[1]],
            ),
            "b": VariableMeta(
                title="Title of Table 1 Variable b",
                description="Description of Table 1 Variable b",
                sources=[sources[2], sources[3]],
                licenses=[licenses[2], licenses[3]],
            ),
        },
    )
    return tb1


@pytest.fixture
def table_2(sources, licenses):
    tb2 = Table(
        {"country": ["Spain", "France", "France"], "year": [2020, 2021, 2022], "a": [10, 20, 30], "c": [40, 50, 60]}
    )
    tb2.metadata = TableMeta(title="Title of Table 2", description="Description of Table 2")
    tb2._fields = defaultdict(
        VariableMeta,
        {
            "a": VariableMeta(
                title="Title of Table 2 Variable a",
                description="Description of Table 2 Variable a",
                sources=[sources[2]],
                licenses=[licenses[2]],
            ),
            "c": VariableMeta(
                title="Title of Table 2 Variable c",
                description="Description of Table 2 Variable c",
                sources=[sources[2], sources[4]],
                licenses=[licenses[4], licenses[2]],
            ),
        },
    )
    return tb2


def _assert_untouched_data_and_metadata_did_not_change(tb1, tb1_expected):
    # Check that all columns that were in the old table have not been affected.
    for column in tb1_expected.columns:
        assert (tb1[column] == tb1_expected[column]).all()
        assert tb1._fields[column] == tb1_expected._fields[column]


def test_create_new_variable_as_sum_of_other_two(table_1, sources, licenses) -> None:
    tb1 = table_1.copy()
    tb1["c"] = tb1["a"] + tb1["b"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["c"] == pd.Series([5, 7, 9])).all()
    assert tb1["c"].metadata.title is None
    assert tb1["c"].metadata.description is None
    assert tb1["c"].metadata.sources == [sources[2], sources[1], sources[3]]
    assert tb1["c"].metadata.licenses == [licenses[1], licenses[2], licenses[3]]


def test_create_new_variable_as_sum_of_another_variable_plus_a_scalar(table_1) -> None:
    tb1 = table_1.copy()
    tb1["d"] = tb1["a"] + 1
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["d"] == pd.Series([2, 3, 4])).all()
    assert tb1["d"].metadata.title == table_1["a"].metadata.title
    assert tb1["d"].metadata.description == table_1["a"].metadata.description
    assert tb1["d"].metadata.sources == table_1["a"].metadata.sources
    assert tb1["d"].metadata.licenses == table_1["a"].metadata.licenses


def test_replace_a_variables_own_value(table_1) -> None:
    tb1 = table_1.copy()
    tb1["a"] = tb1["a"] + 1
    assert (tb1["a"] == pd.Series([2, 3, 4])).all()
    # Metadata for "a" and "b" should be identical.
    assert tb1._fields["a"] == table_1._fields["a"]
    assert tb1._fields["b"] == table_1._fields["b"]


def test_create_new_variable_as_product_of_other_two(table_1, sources, licenses) -> None:
    tb1 = table_1.copy()
    tb1["e"] = tb1["a"] * tb1["b"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["e"] == pd.Series([4, 10, 18])).all()
    assert tb1["e"].metadata.title is None
    assert tb1["e"].metadata.description is None
    assert tb1["e"].metadata.sources == [sources[2], sources[1], sources[3]]
    assert tb1["e"].metadata.licenses == [licenses[1], licenses[2], licenses[3]]


def test_create_new_variable_as_product_of_other_three(table_1, sources, licenses) -> None:
    tb1 = table_1.copy()
    tb1["c"] = tb1["a"] + tb1["b"]
    tb1["f"] = tb1["a"] * tb1["b"] * tb1["c"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["c"] == pd.Series([5, 7, 9])).all()
    assert (tb1["f"] == pd.Series([20, 70, 162])).all()
    assert tb1["f"].metadata.title is None
    assert tb1["f"].metadata.description is None
    assert tb1["f"].metadata.sources == [sources[2], sources[1], sources[3]]
    assert tb1["f"].metadata.licenses == [licenses[1], licenses[2], licenses[3]]


def test_create_new_variable_as_division_of_other_two(table_1, sources, licenses) -> None:
    tb1 = table_1.copy()
    tb1["g"] = tb1["a"] / tb1["b"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["g"] == pd.Series([0.25, 0.40, 0.50])).all()
    assert tb1["g"].metadata.title is None
    assert tb1["g"].metadata.description is None
    assert tb1["g"].metadata.sources == [sources[2], sources[1], sources[3]]
    assert tb1["g"].metadata.licenses == [licenses[1], licenses[2], licenses[3]]


def test_create_new_variable_as_floor_division_of_other_two(table_1, sources, licenses) -> None:
    tb1 = table_1.copy()
    tb1["h"] = tb1["b"] // tb1["a"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["h"] == pd.Series([4, 2, 2])).all()
    assert tb1["h"].metadata.title is None
    assert tb1["h"].metadata.description is None
    assert tb1["h"].metadata.sources == [sources[2], sources[3], sources[1]]
    assert tb1["h"].metadata.licenses == [licenses[2], licenses[3], licenses[1]]


def test_create_new_variable_as_module_division_of_other_two(table_1, sources, licenses) -> None:
    tb1 = table_1.copy()
    tb1["i"] = tb1["a"] % tb1["b"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["i"] == pd.Series([1, 2, 3])).all()
    assert tb1["i"].metadata.title is None
    assert tb1["i"].metadata.description is None
    assert tb1["i"].metadata.sources == [sources[2], sources[1], sources[3]]
    assert tb1["i"].metadata.licenses == [licenses[1], licenses[2], licenses[3]]


def test_create_new_variable_as_another_variable_to_the_power_of_a_scalar(table_1, sources, licenses) -> None:
    tb1 = table_1.copy()
    tb1["j"] = tb1["a"] ** 2
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["j"] == pd.Series([1, 4, 9])).all()
    assert tb1["j"].metadata.title is None
    assert tb1["j"].metadata.description is None
    assert tb1["j"].metadata.sources == [sources[2], sources[1]]
    assert tb1["j"].metadata.licenses == [licenses[1]]


def test_create_new_variables_as_another_variable_to_the_power_of_another_variable(table_1, sources, licenses) -> None:
    tb1 = table_1.copy()
    tb1["k"] = tb1["a"] ** tb1["b"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["k"] == pd.Series([1, 32, 729])).all()
    assert tb1["k"].metadata.title is None
    assert tb1["k"].metadata.description is None
    assert tb1["k"].metadata.sources == [sources[2], sources[1], sources[3]]
    assert tb1["k"].metadata.licenses == [licenses[1], licenses[2], licenses[3]]

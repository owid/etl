#
#  test_variables
#

import pandas as pd
import pytest

from owid.catalog.meta import VariableMeta
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


def test_all_operations() -> None:
    # TODO: Properly split into separate tests.

    # Define metadata that should not change.
    table_1_description = "Description of Table 1"
    table_1_variable_a_title = "Title of Table 1 Variable a"
    table_1_variable_a_description = "Description of Table 1 Variable a"
    table_1_variable_b_title = "Title of Table 1 Variable b"
    table_1_variable_b_description = "Description of Table 1 Variable b"
    source_1 = Source(name="Name of Source 1", description="Description of Source 1")
    source_2 = Source(name="Name of Source 2", description="Description of Source 2")
    source_3 = Source(name="Name of Source 3", description="Description of Source 3")
    license_1 = License(name="Name of License 1", url="URL of License 1")
    license_2 = License(name="Name of License 2", url="URL of License 2")
    license_3 = License(name="Name of License 3", url="URL of License 3")
    # Create a table with the above metadata and some mock data.
    tb1 = Table({"country": ["Spain", "Spain", "France"], "year": [2020, 2021, 2021], "a": [1, 2, 3], "b": [4, 5, 6]})
    tb1.metadata.description = table_1_description
    tb1["a"].metadata.title = table_1_variable_a_title
    tb1["a"].metadata.description = table_1_variable_a_description
    tb1["b"].metadata.title = table_1_variable_b_title
    tb1["b"].metadata.description = table_1_variable_b_description
    tb1["a"].metadata.sources = [source_2, source_1]
    tb1["b"].metadata.sources = [source_2, source_3]
    tb1["a"].metadata.licenses = [license_1]
    tb1["b"].metadata.licenses = [license_2, license_3]

    def _assert_untouched_data_and_metadata_did_not_change(tb1):
        assert (tb1["a"] == pd.Series([1, 2, 3])).all()
        assert (tb1["b"] == pd.Series([4, 5, 6])).all()
        assert tb1.metadata.description == table_1_description
        assert tb1["a"].metadata.title == table_1_variable_a_title
        assert tb1["b"].metadata.title == table_1_variable_b_title
        assert tb1["a"].metadata.description == table_1_variable_a_description
        assert tb1["b"].metadata.description == table_1_variable_b_description
        assert tb1["a"].metadata.sources == [source_2, source_1]
        assert tb1["b"].metadata.sources == [source_2, source_3]
        assert tb1["a"].metadata.licenses == [license_1]
        assert tb1["b"].metadata.licenses == [license_2, license_3]

    # Create a new variable as the sum of another two variables.
    tb1["c"] = tb1["a"] + tb1["b"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1)
    assert (tb1["c"] == pd.Series([5, 7, 9])).all()
    assert tb1["c"].metadata.title is None
    assert tb1["c"].metadata.description is None
    assert tb1["c"].metadata.sources == [source_2, source_1, source_3]
    assert tb1["c"].metadata.licenses == [license_1, license_2, license_3]

    # Create a new variables as the sum of another variable plus a scalar.
    tb1["d"] = tb1["a"] + 1
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1)
    assert (tb1["d"] == pd.Series([2, 3, 4])).all()
    assert tb1["d"].metadata.title == table_1_variable_a_title
    assert tb1["d"].metadata.description == table_1_variable_a_description
    assert tb1["d"].metadata.sources == [source_2, source_1]
    assert tb1["d"].metadata.licenses == [license_1]

    # Replace a variables' own value.
    tb1["d"] = tb1["d"] + 1
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1)
    assert (tb1["d"] == pd.Series([3, 4, 5])).all()
    assert tb1["d"].metadata.title == table_1_variable_a_title
    assert tb1["d"].metadata.description == table_1_variable_a_description
    assert tb1["d"].metadata.sources == [source_2, source_1]
    assert tb1["d"].metadata.licenses == [license_1]

    tb1["e"] = tb1["a"] * tb1["b"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1)
    assert (tb1["e"] == pd.Series([4, 10, 18])).all()
    assert tb1["e"].metadata.title is None
    assert tb1["e"].metadata.description is None
    assert tb1["e"].metadata.sources == [source_2, source_1, source_3]
    assert tb1["e"].metadata.licenses == [license_1, license_2, license_3]

    tb1["f"] = tb1["a"] * tb1["b"] * tb1["c"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1)
    assert (tb1["c"] == pd.Series([5, 7, 9])).all()
    assert (tb1["f"] == pd.Series([20, 70, 162])).all()
    assert tb1["f"].metadata.title is None
    assert tb1["f"].metadata.description is None
    assert tb1["f"].metadata.sources == [source_2, source_1, source_3]
    assert tb1["f"].metadata.licenses == [license_1, license_2, license_3]

    tb1["g"] = tb1["a"] / tb1["b"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1)
    assert (tb1["g"] == pd.Series([0.25, 0.40, 0.50])).all()
    assert tb1["g"].metadata.title is None
    assert tb1["g"].metadata.description is None
    assert tb1["g"].metadata.sources == [source_2, source_1, source_3]
    assert tb1["g"].metadata.licenses == [license_1, license_2, license_3]

    tb1["h"] = tb1["b"] // tb1["a"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1)
    assert (tb1["h"] == pd.Series([4, 2, 2])).all()
    assert tb1["h"].metadata.title is None
    assert tb1["h"].metadata.description is None
    assert tb1["h"].metadata.sources == [source_2, source_3, source_1]
    assert tb1["h"].metadata.licenses == [license_2, license_3, license_1]

    # Create a new variable as another variable to the power of a scalar
    tb1["j"] = tb1["a"] ** 2
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1)
    assert (tb1["j"] == pd.Series([1, 4, 9])).all()
    assert tb1["j"].metadata.title is None
    assert tb1["j"].metadata.description is None
    assert tb1["j"].metadata.sources == [source_2, source_1]
    assert tb1["j"].metadata.licenses == [license_1]

    # Create a new variable as another variable to the power of another variable.
    tb1["k"] = tb1["a"] ** tb1["b"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1)
    assert (tb1["k"] == pd.Series([1, 32, 729])).all()
    assert tb1["k"].metadata.title is None
    assert tb1["k"].metadata.description is None
    assert tb1["k"].metadata.sources == [source_2, source_1, source_3]
    assert tb1["k"].metadata.licenses == [license_1, license_2, license_3]

    tb1["i"] = tb1["a"] % tb1["b"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1)
    assert (tb1["i"] == pd.Series([1, 2, 3])).all()
    assert tb1["i"].metadata.title is None
    assert tb1["i"].metadata.description is None
    assert tb1["i"].metadata.sources == [source_2, source_1, source_3]
    assert tb1["i"].metadata.licenses == [license_1, license_2, license_3]

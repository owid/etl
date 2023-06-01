#
#  test_variables
#

import pytest

from owid.catalog.meta import VariableMeta
from owid.catalog.variables import Variable


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

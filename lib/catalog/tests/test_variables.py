#
#  test_variables
#

import pandas as pd
import pytest

from owid.catalog.meta import VariableMeta
from owid.catalog.variables import (
    Variable,
    combine_variables_metadata,
    get_unique_licenses_from_variables,
    get_unique_origins_from_variables,
    get_unique_sources_from_variables,
)


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


def test_operations_on_unnamed_variables_succeed() -> None:
    # we currently rely on this for interim logic in the ETL
    v = Variable([1, 2, 3])
    v2 = v + 1  # noqa


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


def _assert_untouched_data_and_metadata_did_not_change(tb1, tb1_expected):
    # Check that all columns that were in the old table have not been affected.
    for column in tb1_expected.columns:
        assert (tb1[column] == tb1_expected[column]).all()
        assert tb1._fields[column] == tb1_expected._fields[column]


def test_create_new_variable_as_sum_of_other_two(table_1, sources, origins, licenses) -> None:
    tb1 = table_1.copy()
    tb1["c"] = tb1["a"] + tb1["b"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["c"] == pd.Series([5, 7, 9])).all()
    # Since "a" and "b" have different title and description, "c" should have no title or description.
    assert tb1["c"].metadata.title is None
    assert tb1["c"].metadata.description is None
    assert tb1["c"].metadata.sources == [sources[2], sources[1], sources[3]]
    assert tb1["c"].metadata.origins == [origins[2], origins[1], origins[3]]
    assert tb1["c"].metadata.licenses == [licenses[1], licenses[2], licenses[3]]
    # Processing level should be the highest of the two variables.
    assert tb1["c"].metadata.processing_level == "major"
    # Both "a" and "b" have different values in presentation, the combination should have no presentation.
    assert tb1["c"].metadata.presentation is None
    # Since "a" and "b" have identical display, the combination should have the same display.
    assert tb1["c"].metadata.display == tb1["a"].metadata.display


def test_create_new_variable_as_sum_of_another_variable_plus_a_scalar(table_1) -> None:
    tb1 = table_1.copy()
    tb1["d"] = tb1["a"] + 1
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["d"] == pd.Series([2, 3, 4])).all()
    assert tb1["d"].metadata.title == table_1["a"].metadata.title
    assert tb1["d"].metadata.description == table_1["a"].metadata.description
    assert tb1["d"].metadata.sources == table_1["a"].metadata.sources
    assert tb1["d"].metadata.origins == table_1["a"].metadata.origins
    assert tb1["d"].metadata.licenses == table_1["a"].metadata.licenses
    assert tb1["d"].metadata.processing_level == "minor"
    assert tb1["d"].metadata.presentation == tb1["a"].metadata.presentation
    assert tb1["d"].metadata.display == tb1["a"].metadata.display


def test_replace_a_variables_own_value(table_1) -> None:
    tb1 = table_1.copy()
    tb1["a"] = tb1["a"] + 1
    assert (tb1["a"] == pd.Series([2, 3, 4])).all()
    # Metadata for "a" and "b" should be identical.
    assert tb1._fields["a"] == table_1._fields["a"]
    assert tb1._fields["b"] == table_1._fields["b"]

    # Idem using +=.
    tb1 = table_1.copy()
    tb1["a"] += 1
    assert (tb1["a"] == pd.Series([2, 3, 4])).all()
    # Metadata for "a" and "b" should be identical.
    assert tb1._fields["a"] == table_1._fields["a"]
    assert tb1._fields["b"] == table_1._fields["b"]


def test_operations_of_variable_and_scalar(table_1):
    table_1_original = table_1.copy()
    table_1["a"] = table_1["a"] + 1
    table_1["a"] += 1
    table_1["a"] = table_1["a"] - 1
    table_1["a"] -= 1
    table_1["a"] = table_1["a"] * 1
    table_1["a"] *= 1
    table_1["a"] = table_1["a"] / 1
    table_1["a"] /= 1
    table_1["a"] = table_1["a"] // 1
    table_1["a"] //= 1
    table_1["a"] = table_1["a"] % 1
    table_1["a"] %= 1
    table_1["a"] = table_1["a"] ** 1
    table_1["a"] **= 1

    # None of the operations above should have affected the metadata.
    assert table_1["a"].metadata == table_1_original["a"].metadata


def test_create_new_variable_as_product_of_other_two(table_1, sources, origins, licenses) -> None:
    tb1 = table_1.copy()
    tb1["e"] = tb1["a"] * tb1["b"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["e"] == pd.Series([4, 10, 18])).all()
    assert tb1["e"].metadata.title is None
    assert tb1["e"].metadata.description is None
    assert tb1["e"].metadata.sources == [sources[2], sources[1], sources[3]]
    assert tb1["e"].metadata.origins == [origins[2], origins[1], origins[3]]
    assert tb1["e"].metadata.licenses == [licenses[1], licenses[2], licenses[3]]
    # Processing level should be the highest of the two variables.
    assert tb1["e"].metadata.processing_level == "major"
    # Both "a" and "b" have different values in presentation, the combination should have no presentation.
    assert tb1["e"].metadata.presentation is None
    # Since "a" and "b" have identical display, the combination should have the same display.
    assert tb1["e"].metadata.display == tb1["a"].metadata.display


def test_create_new_variable_as_product_of_other_three(table_1, sources, origins, licenses) -> None:
    tb1 = table_1.copy()
    tb1["c"] = tb1["a"] + tb1["b"]
    tb1["f"] = tb1["a"] * tb1["b"] * tb1["c"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["c"] == pd.Series([5, 7, 9])).all()
    assert (tb1["f"] == pd.Series([20, 70, 162])).all()
    assert tb1["f"].metadata.title is None
    assert tb1["f"].metadata.description is None
    assert tb1["f"].metadata.sources == [sources[2], sources[1], sources[3]]
    assert tb1["f"].metadata.origins == [origins[2], origins[1], origins[3]]
    assert tb1["f"].metadata.licenses == [licenses[1], licenses[2], licenses[3]]
    # Processing level should be the highest of all variables.
    assert tb1["c"].metadata.processing_level == "major"
    # Both "a" and "b" have different values in presentation, the combination should have no presentation.
    assert tb1["c"].metadata.presentation is None
    # Since "a" and "b" have identical display (and hence also "c"), the combination should have the same display.
    assert tb1["c"].metadata.display == tb1["a"].metadata.display


def test_create_new_variable_as_division_of_other_two(table_1, sources, origins, licenses) -> None:
    tb1 = table_1.copy()
    tb1["g"] = tb1["a"] / tb1["b"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["g"] == pd.Series([0.25, 0.40, 0.50])).all()
    assert tb1["g"].metadata.title is None
    assert tb1["g"].metadata.description is None
    assert tb1["g"].metadata.sources == [sources[2], sources[1], sources[3]]
    assert tb1["g"].metadata.origins == [origins[2], origins[1], origins[3]]
    assert tb1["g"].metadata.licenses == [licenses[1], licenses[2], licenses[3]]
    # Processing level should be the highest of the two variables.
    assert tb1["g"].metadata.processing_level == "major"
    # Both "a" and "b" have different values in presentation, the combination should have no presentation.
    assert tb1["g"].metadata.presentation is None
    # Since "a" and "b" have identical display, the combination should have the same display.
    assert tb1["g"].metadata.display == tb1["a"].metadata.display


def test_create_new_variable_as_floor_division_of_other_two(table_1, sources, origins, licenses) -> None:
    tb1 = table_1.copy()
    tb1["h"] = tb1["b"] // tb1["a"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["h"] == pd.Series([4, 2, 2])).all()
    assert tb1["h"].metadata.title is None
    assert tb1["h"].metadata.description is None
    assert tb1["h"].metadata.sources == [sources[2], sources[3], sources[1]]
    assert tb1["h"].metadata.origins == [origins[2], origins[3], origins[1]]
    assert tb1["h"].metadata.licenses == [licenses[2], licenses[3], licenses[1]]
    # Processing level should be the highest of the two variables.
    assert tb1["h"].metadata.processing_level == "major"
    # Both "a" and "b" have different values in presentation, the combination should have no presentation.
    assert tb1["h"].metadata.presentation is None
    # Since "a" and "b" have identical display, the combination should have the same display.
    assert tb1["h"].metadata.display == tb1["a"].metadata.display


def test_create_new_variable_as_module_division_of_other_two(table_1, sources, origins, licenses) -> None:
    tb1 = table_1.copy()
    tb1["i"] = tb1["a"] % tb1["b"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["i"] == pd.Series([1, 2, 3])).all()
    assert tb1["i"].metadata.title is None
    assert tb1["i"].metadata.description is None
    assert tb1["i"].metadata.sources == [sources[2], sources[1], sources[3]]
    assert tb1["i"].metadata.origins == [origins[2], origins[1], origins[3]]
    assert tb1["i"].metadata.licenses == [licenses[1], licenses[2], licenses[3]]
    # Processing level should be the highest of the two variables.
    assert tb1["i"].metadata.processing_level == "major"
    # Both "a" and "b" have different values in presentation, the combination should have no presentation.
    assert tb1["i"].metadata.presentation is None
    # Since "a" and "b" have identical display, the combination should have the same display.
    assert tb1["i"].metadata.display == tb1["a"].metadata.display


def test_create_new_variable_as_another_variable_to_the_power_of_a_scalar(table_1, sources, origins, licenses) -> None:
    tb1 = table_1.copy()
    tb1["j"] = tb1["a"] ** 2
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["j"] == pd.Series([1, 4, 9])).all()
    assert tb1["j"].metadata.title == "Title of Table 1 Variable a"
    assert tb1["j"].metadata.description == "Description of Table 1 Variable a"
    assert tb1["j"].metadata.sources == [sources[2], sources[1]]
    assert tb1["j"].metadata.origins == [origins[2], origins[1]]
    assert tb1["j"].metadata.licenses == [licenses[1]]
    assert tb1["j"].metadata.processing_level == "minor"
    assert tb1["j"].metadata.presentation == tb1["a"].metadata.presentation
    assert tb1["j"].metadata.display == tb1["a"].metadata.display


def test_create_new_variables_as_another_variable_to_the_power_of_another_variable(
    table_1, sources, origins, licenses
) -> None:
    tb1 = table_1.copy()
    tb1["k"] = tb1["a"] ** tb1["b"]
    _assert_untouched_data_and_metadata_did_not_change(tb1=tb1, tb1_expected=table_1)
    assert (tb1["k"] == pd.Series([1, 32, 729])).all()
    assert tb1["k"].metadata.title is None
    assert tb1["k"].metadata.description is None
    assert tb1["k"].metadata.sources == [sources[2], sources[1], sources[3]]
    assert tb1["k"].metadata.origins == [origins[2], origins[1], origins[3]]
    assert tb1["k"].metadata.licenses == [licenses[1], licenses[2], licenses[3]]
    # Processing level should be the highest of the two variables.
    assert tb1["k"].metadata.processing_level == "major"
    # Both "a" and "b" have different values in presentation, the combination should have no presentation.
    assert tb1["k"].metadata.presentation is None
    # Since "a" and "b" have identical display, the combination should have the same display.
    assert tb1["k"].metadata.display == tb1["a"].metadata.display


def test_get_unique_sources_from_variables(variable_1, variable_2, sources):
    assert get_unique_sources_from_variables([variable_1, variable_2]) == [sources[2], sources[1], sources[3]]
    # Ensure that the function respects the order in which sources appear.
    assert get_unique_sources_from_variables([variable_2, variable_1]) == [sources[2], sources[3], sources[1]]


def test_get_unique_origins_from_variables(variable_1, variable_2, origins):
    assert get_unique_origins_from_variables([variable_1, variable_2]) == [origins[2], origins[1], origins[3]]
    # Ensure that the function respects the order in which origins appear.
    assert get_unique_origins_from_variables([variable_2, variable_1]) == [origins[2], origins[3], origins[1]]


def test_get_unique_licenses_from_variables(variable_1, variable_2, licenses):
    assert get_unique_licenses_from_variables([variable_1, variable_2]) == [licenses[1], licenses[2], licenses[3]]
    # Ensure that the function respects the order in which sources appear.
    assert get_unique_licenses_from_variables([variable_2, variable_1]) == [licenses[2], licenses[3], licenses[1]]


def test_combine_variables_metadata_with_different_fields(variable_1, variable_2, sources, origins, licenses) -> None:
    variable_1 = variable_1.copy()
    variable_2 = variable_2.copy()
    for operation in ["+", "-", "melt", "pivot", "concat"]:
        # TODO: Assert this raises a warning because units are different.
        metadata = combine_variables_metadata([variable_1, variable_2], operation=operation)  # type: ignore
        # If titles/descriptions/units/short_units are different, they should not be propagated.
        assert metadata.title is None
        assert metadata.description is None
        assert metadata.unit is None
        assert metadata.short_unit is None
        assert metadata.sources == [sources[2], sources[1], sources[3]]
        assert metadata.origins == [origins[2], origins[1], origins[3]]
        assert metadata.licenses == [licenses[1], licenses[2], licenses[3]]
        # variable_2 has a major processing level, so the combined variable should have a major processing level.
        assert metadata.processing_level == "major"
        # Both variables have different values in presentation, so the combination should have no presentation.
        assert metadata.presentation is None
        # Since both variables have identical display, the combination should have the same display.
        assert metadata.display == variable_1.metadata.display


def test_combine_variables_metadata_with_equal_fields(variable_1, variable_2) -> None:
    variable_1 = variable_1.copy()
    # Impose that variable 2 is identical to 1.
    variable_2 = variable_1.copy()
    for operation in ["+", "-", "melt", "pivot", "concat"]:
        metadata = combine_variables_metadata([variable_1, variable_2], operation=operation)  # type: ignore
        # If titles/descriptions/units/short_units are identical, they should be propagated.
        assert metadata.title == variable_1.metadata.title
        assert metadata.description == variable_1.metadata.description
        assert metadata.unit == variable_1.metadata.unit
        assert metadata.short_unit == variable_1.metadata.short_unit
        assert metadata.sources == variable_1.metadata.sources
        assert metadata.origins == variable_1.metadata.origins
        assert metadata.licenses == variable_2.metadata.licenses
        # Now both variables have the same processing level, which is minor.
        assert metadata.processing_level == "minor"
        # Both variables have the same presentation, so the combination should have the same presentation.
        assert metadata.presentation == variable_1.metadata.presentation
        # Since both variables have identical display, the combination should have the same display.
        assert metadata.display == variable_1.metadata.display


def test_dropna(table_1) -> None:
    tb1 = table_1.copy()
    tb1.loc[1, "b"] = pd.NA
    new_var = tb1["b"].dropna()
    assert (new_var == pd.Series([4, 6], index=[0, 2])).all()
    # Check that metadata of the new variable coincides with that of the original.
    assert new_var.metadata == tb1["b"].metadata
    # Check that table's metadata is not affected.
    assert tb1.metadata == table_1.metadata
    # Check that the original variable's metadata is not affected.
    assert tb1["b"].metadata == table_1["b"].metadata

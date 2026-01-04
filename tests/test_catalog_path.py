"""Tests for CatalogPath class."""

import pytest
from owid.catalog.core import CatalogPath


class TestFromStr:
    """Tests for CatalogPath.from_str parsing."""

    def test_parse_full_path(self) -> None:
        """Parse a complete catalog path with all components."""
        p = CatalogPath.from_str("grapher/who/2024-01-15/gho/life_expectancy#value")
        assert p.channel == "grapher"
        assert p.namespace == "who"
        assert p.version == "2024-01-15"
        assert p.dataset == "gho"
        assert p.table == "life_expectancy"
        assert p.variable == "value"

    def test_parse_without_variable(self) -> None:
        """Parse path without variable component."""
        p = CatalogPath.from_str("grapher/who/2024/gho/life_expectancy")
        assert p.table == "life_expectancy"
        assert p.variable is None

    def test_parse_dataset_only(self) -> None:
        """Parse path with only dataset (no table/variable)."""
        p = CatalogPath.from_str("meadow/un/2024-06-01/population")
        assert p.channel == "meadow"
        assert p.namespace == "un"
        assert p.version == "2024-06-01"
        assert p.dataset == "population"
        assert p.table is None
        assert p.variable is None

    def test_parse_year_only_version(self) -> None:
        """Parse path with year-only version."""
        p = CatalogPath.from_str("garden/worldbank/2024/wdi/gdp#value")
        assert p.version == "2024"

    def test_parse_latest_version(self) -> None:
        """Parse path with 'latest' version."""
        p = CatalogPath.from_str("grapher/owid/latest/key_indicators/population#value")
        assert p.version == "latest"

    def test_parse_with_uri_prefix(self) -> None:
        """Parse path with data:// URI prefix."""
        p = CatalogPath.from_str("data://grapher/who/2024/gho/table#var")
        assert p.channel == "grapher"
        assert p.namespace == "who"
        assert p.table == "table"
        assert p.variable == "var"

    def test_parse_snapshot_uri(self) -> None:
        """Parse snapshot:// URI."""
        p = CatalogPath.from_str("snapshot://who/2024-01-01/data")
        assert p.channel == "snapshot"
        assert p.namespace == "who"
        assert p.version == "2024-01-01"
        assert p.dataset == "data"

    def test_parse_with_hyphens_in_names(self) -> None:
        """Names can contain hyphens."""
        p = CatalogPath.from_str("grapher/energy-data/2024/primary-energy/coal-production#value")
        assert p.namespace == "energy-data"
        assert p.dataset == "primary-energy"
        assert p.table == "coal-production"


class TestFromStrValidation:
    """Tests for validation in from_str."""

    def test_invalid_channel(self) -> None:
        """Reject invalid channel."""
        with pytest.raises(ValueError, match="Invalid channel"):
            CatalogPath.from_str("invalid/who/2024/gho/table#var")

    def test_invalid_version_format(self) -> None:
        """Reject invalid version format."""
        with pytest.raises(ValueError, match="Invalid version"):
            CatalogPath.from_str("grapher/who/2024-1-1/gho/table#var")

    def test_invalid_path_too_few_parts(self) -> None:
        """Reject path with too few parts."""
        with pytest.raises(ValueError, match="Invalid catalog path"):
            CatalogPath.from_str("grapher/who/2024")

    def test_invalid_path_too_many_parts(self) -> None:
        """Reject path with too many parts."""
        with pytest.raises(ValueError, match="Invalid catalog path"):
            CatalogPath.from_str("grapher/who/2024/gho/table/extra")

    def test_invalid_name_characters(self) -> None:
        """Reject names with invalid characters."""
        with pytest.raises(ValueError, match="Invalid namespace"):
            CatalogPath.from_str("grapher/who@org/2024/gho/table")


class TestProperties:
    """Tests for CatalogPath properties."""

    def test_short_name(self) -> None:
        """short_name is alias for dataset."""
        p = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        assert p.short_name == "gho"
        assert p.short_name == p.dataset

    def test_dataset_path(self) -> None:
        """dataset_path returns path to dataset."""
        p = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        assert p.dataset_path == "grapher/who/2024/gho"

    def test_table_path(self) -> None:
        """table_path returns path to table."""
        p = CatalogPath.from_str("grapher/who/2024/gho/life_expectancy#value")
        assert p.table_path == "grapher/who/2024/gho/life_expectancy"

    def test_table_path_none_when_no_table(self) -> None:
        """table_path is None when no table."""
        p = CatalogPath.from_str("grapher/who/2024/gho")
        assert p.table_path is None

    def test_step_uri(self) -> None:
        """step_uri returns data:// URI."""
        p = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        assert p.step_uri == "data://grapher/who/2024/gho"

    def test_table_variable(self) -> None:
        """table_variable returns table#variable slug."""
        p = CatalogPath.from_str("grapher/who/2024/gho/life_expectancy#value")
        assert p.table_variable == "life_expectancy#value"

    def test_table_variable_without_variable(self) -> None:
        """table_variable returns just table when no variable."""
        p = CatalogPath.from_str("grapher/who/2024/gho/life_expectancy")
        assert p.table_variable == "life_expectancy"

    def test_table_variable_none_when_no_table(self) -> None:
        """table_variable is None when no table."""
        p = CatalogPath.from_str("grapher/who/2024/gho")
        assert p.table_variable is None


class TestWithMethods:
    """Tests for with_* builder methods."""

    def test_with_version(self) -> None:
        """with_version creates new path with different version."""
        p = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        p2 = p.with_version("2025-01-01")
        assert p2.version == "2025-01-01"
        assert p.version == "2024"  # Original unchanged

    def test_with_channel(self) -> None:
        """with_channel creates new path with different channel."""
        p = CatalogPath.from_str("meadow/who/2024/gho/table")
        p2 = p.with_channel("garden")
        assert p2.channel == "garden"
        assert p.channel == "meadow"

    def test_with_variable(self) -> None:
        """with_variable creates new path with different variable."""
        p = CatalogPath.from_str("grapher/who/2024/gho/table#old_var")
        p2 = p.with_variable("new_var")
        assert p2.variable == "new_var"
        assert p.variable == "old_var"

    def test_with_variable_requires_table(self) -> None:
        """with_variable raises if no table."""
        p = CatalogPath.from_str("grapher/who/2024/gho")
        with pytest.raises(ValueError, match="Cannot set variable without table"):
            p.with_variable("var")

    def test_with_table(self) -> None:
        """with_table creates new path with different table."""
        p = CatalogPath.from_str("grapher/who/2024/gho/old_table#var")
        p2 = p.with_table("new_table")
        assert p2.table == "new_table"
        assert p2.variable == "var"  # Variable preserved

    def test_with_table_none_clears_variable(self) -> None:
        """Setting table to None also clears variable."""
        p = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        p2 = p.with_table(None)
        assert p2.table is None
        assert p2.variable is None

    def test_without_variable(self) -> None:
        """without_variable removes variable."""
        p = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        p2 = p.without_variable()
        assert p2.variable is None
        assert p2.table == "table"  # Table preserved

    def test_without_table(self) -> None:
        """without_table removes table and variable."""
        p = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        p2 = p.without_table()
        assert p2.table is None
        assert p2.variable is None


class TestStringRepresentation:
    """Tests for __str__ and __repr__."""

    def test_str_full_path(self) -> None:
        """__str__ returns full path."""
        p = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        assert str(p) == "grapher/who/2024/gho/table#var"

    def test_str_without_variable(self) -> None:
        """__str__ without variable."""
        p = CatalogPath.from_str("grapher/who/2024/gho/table")
        assert str(p) == "grapher/who/2024/gho/table"

    def test_str_dataset_only(self) -> None:
        """__str__ dataset only."""
        p = CatalogPath.from_str("grapher/who/2024/gho")
        assert str(p) == "grapher/who/2024/gho"

    def test_repr(self) -> None:
        """__repr__ includes class name."""
        p = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        assert repr(p) == "CatalogPath('grapher/who/2024/gho/table#var')"

    def test_roundtrip(self) -> None:
        """Parsing str() output gives equivalent path."""
        original = "grapher/who/2024-01-15/gho/life_expectancy#value"
        p = CatalogPath.from_str(original)
        p2 = CatalogPath.from_str(str(p))
        assert p == p2


class TestHashingAndEquality:
    """Tests for hashing and equality (frozen dataclass)."""

    def test_equality(self) -> None:
        """Equal paths are equal."""
        p1 = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        p2 = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        assert p1 == p2

    def test_inequality(self) -> None:
        """Different paths are not equal."""
        p1 = CatalogPath.from_str("grapher/who/2024/gho/table#var1")
        p2 = CatalogPath.from_str("grapher/who/2024/gho/table#var2")
        assert p1 != p2

    def test_hashable(self) -> None:
        """Can be used as dict key."""
        p = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        d = {p: "value"}
        assert d[p] == "value"

    def test_usable_in_set(self) -> None:
        """Can be added to set."""
        p1 = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        p2 = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        s = {p1, p2}
        assert len(s) == 1

    def test_equality_with_string(self) -> None:
        """Can compare with string directly."""
        p = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        assert p == "grapher/who/2024/gho/table#var"
        assert p != "grapher/who/2024/gho/table#other"

    def test_equality_string_both_directions(self) -> None:
        """String comparison works in both directions."""
        p = CatalogPath.from_str("grapher/who/2024/gho")
        assert p == "grapher/who/2024/gho"
        # Note: "string" == CatalogPath won't work (string's __eq__ is called)
        # but CatalogPath == "string" works


class TestParentProperty:
    """Tests for parent property."""

    def test_parent_removes_variable(self) -> None:
        """Parent of path with variable removes variable."""
        p = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        assert p.parent == "grapher/who/2024/gho/table"
        assert p.parent.variable is None
        assert p.parent.table == "table"

    def test_parent_removes_table(self) -> None:
        """Parent of path with table (no variable) removes table."""
        p = CatalogPath.from_str("grapher/who/2024/gho/table")
        assert p.parent == "grapher/who/2024/gho"
        assert p.parent.table is None

    def test_parent_of_dataset_is_self(self) -> None:
        """Parent of dataset-only path is itself (same object)."""
        p = CatalogPath.from_str("grapher/who/2024/gho")
        assert p.parent == p
        assert p.parent is p  # Same object returned

    def test_parent_chain(self) -> None:
        """Can chain parent calls."""
        p = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        assert p.parent.parent == "grapher/who/2024/gho"


class TestTruedivOperator:
    """Tests for / operator (path joining)."""

    def test_append_table(self) -> None:
        """Can append table with / operator."""
        p = CatalogPath.from_str("grapher/who/2024/gho")
        p2 = p / "life_expectancy"
        assert p2 == "grapher/who/2024/gho/life_expectancy"
        assert p2.table == "life_expectancy"
        assert p2.variable is None

    def test_append_table_and_variable(self) -> None:
        """Can append table#variable with / operator."""
        p = CatalogPath.from_str("grapher/who/2024/gho")
        p2 = p / "life_expectancy#value"
        assert p2 == "grapher/who/2024/gho/life_expectancy#value"
        assert p2.table == "life_expectancy"
        assert p2.variable == "value"

    def test_append_to_table_raises(self) -> None:
        """Cannot append to path that already has table."""
        p = CatalogPath.from_str("grapher/who/2024/gho/table")
        with pytest.raises(ValueError, match="Cannot append"):
            _ = p / "another"


class TestFspathProtocol:
    """Tests for os.fspath() compatibility."""

    def test_fspath_returns_dataset_path(self) -> None:
        """__fspath__ returns dataset_path for use with Path."""
        import os

        p = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        assert os.fspath(p) == "grapher/who/2024/gho"

    def test_works_with_pathlib(self) -> None:
        """Can use with pathlib.Path / operator."""
        from pathlib import Path

        base = Path("/data")
        p = CatalogPath.from_str("grapher/who/2024/gho/table#var")
        result = base / p
        assert result == Path("/data/grapher/who/2024/gho")

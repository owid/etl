"""Utilities for working with OWID catalog paths."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from typing import Literal, cast, get_args

# Available channels in the catalog (matches owid.catalog.datasets.CHANNEL)
CHANNEL = Literal[
    "snapshot",
    "garden",
    "meadow",
    "grapher",
    "open_numbers",
    "examples",
    "explorers",
    "external",
    "multidim",
]

# Set of valid channels for runtime validation
VALID_CHANNELS: frozenset[str] = frozenset(get_args(CHANNEL))

# Regex for validating version formats: YYYY-MM-DD, YYYY, or "latest"
VERSION_PATTERN = re.compile(r"^(?:\d{4}-\d{2}-\d{2}|\d{4}|latest)$")

# Regex for valid names (alphanumeric + underscores + hyphens)
NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True)
class CatalogPath:
    """
    Parse and manipulate OWID catalog paths like pathlib.Path.

    Catalog paths follow this format:
        channel/namespace/version/dataset/table#variable

    Examples:
        >>> p = CatalogPath.from_str("grapher/who/2024-01-15/gho/life_expectancy#value")
        >>> p.channel
        'grapher'
        >>> p.namespace
        'who'
        >>> p.dataset_path
        'grapher/who/2024-01-15/gho'
        >>> p.step_uri
        'data://grapher/who/2024-01-15/gho'

        >>> p2 = p.with_version("2025-01-01")
        >>> str(p2)
        'grapher/who/2025-01-01/gho/life_expectancy#value'
    """

    channel: CHANNEL
    namespace: str
    version: str
    dataset: str
    table: str | None = None
    variable: str | None = None

    def __post_init__(self) -> None:
        """Validate path components."""
        if self.channel not in VALID_CHANNELS:
            raise ValueError(f"Invalid channel '{self.channel}'. Must be one of: {sorted(VALID_CHANNELS)}")

        if not VERSION_PATTERN.match(self.version):
            raise ValueError(f"Invalid version '{self.version}'. Must be YYYY-MM-DD, YYYY, or 'latest'")

        for name, value in [("namespace", self.namespace), ("dataset", self.dataset)]:
            if not NAME_PATTERN.match(value):
                raise ValueError(f"Invalid {name} '{value}'. Must be alphanumeric with underscores/hyphens")

        if self.table is not None and not NAME_PATTERN.match(self.table):
            raise ValueError(f"Invalid table '{self.table}'. Must be alphanumeric with underscores/hyphens")

        if self.variable is not None and not NAME_PATTERN.match(self.variable):
            raise ValueError(f"Invalid variable '{self.variable}'. Must be alphanumeric with underscores/hyphens")

        if self.variable is not None and self.table is None:
            raise ValueError("Cannot have variable without table")

    @classmethod
    def from_str(cls, path: str) -> CatalogPath:
        """
        Parse a catalog path string.

        Supported formats:
            - Full: channel/namespace/version/dataset/table#variable
            - Without variable: channel/namespace/version/dataset/table
            - Dataset only: channel/namespace/version/dataset
            - Snapshot: snapshot://namespace/version/short_name (3 parts, channel implicit)

        Args:
            path: Catalog path string to parse

        Returns:
            CatalogPath instance

        Raises:
            ValueError: If path format is invalid
        """
        # Handle URI prefix - snapshot:// has implicit channel
        channel_from_uri: str | None = None
        if "://" in path:
            prefix, path = path.split("://", 1)
            # For snapshot:// and snapshot-private://, channel is implicit
            if prefix in ("snapshot", "snapshot-private"):
                channel_from_uri = "snapshot"
            # For data://, channel is in the path

        # Split variable from path
        variable: str | None = None
        if "#" in path:
            path, variable = path.split("#", 1)

        parts = path.split("/")

        # Snapshot paths: namespace/version/short_name (3 parts)
        if len(parts) == 3 and channel_from_uri == "snapshot":
            if variable is not None:
                raise ValueError("Snapshot paths cannot have indicators (# component)")
            namespace, version, dataset = parts
            return cls(
                channel="snapshot",
                namespace=namespace,
                version=version,
                dataset=dataset,
            )
        elif len(parts) == 4:
            # Dataset-only path: channel/namespace/version/dataset
            channel, namespace, version, dataset = parts
            return cls(
                channel=cast(CHANNEL, channel),
                namespace=namespace,
                version=version,
                dataset=dataset,
            )
        elif len(parts) == 5:
            # Full path: channel/namespace/version/dataset/table
            channel, namespace, version, dataset, table = parts
            return cls(
                channel=cast(CHANNEL, channel),
                namespace=namespace,
                version=version,
                dataset=dataset,
                table=table,
                variable=variable,
            )
        else:
            raise ValueError(
                f"Invalid catalog path '{path}'. Expected format: "
                "channel/namespace/version/dataset[/table][#variable] or "
                "snapshot://namespace/version/short_name"
            )

    @classmethod
    def from_uri(cls, uri: str) -> CatalogPath:
        """
        Parse a step URI like 'data://grapher/who/2024/dataset'.

        Args:
            uri: Step URI to parse

        Returns:
            CatalogPath instance
        """
        return cls.from_str(uri)

    @property
    def short_name(self) -> str:
        """Alias for dataset (common usage in codebase)."""
        return self.dataset

    @property
    def dataset_path(self) -> str:
        """Path to dataset: channel/namespace/version/dataset."""
        return f"{self.channel}/{self.namespace}/{self.version}/{self.dataset}"

    @property
    def table_path(self) -> str | None:
        """Full path to table: channel/namespace/version/dataset/table."""
        if self.table is None:
            return None
        return f"{self.dataset_path}/{self.table}"

    @property
    def step_uri(self) -> str:
        """ETL step URI: data://channel/namespace/version/dataset."""
        return f"data://{self.dataset_path}"

    @property
    def table_variable(self) -> str | None:
        """Table and variable slug: table#variable (or just table if no variable)."""
        if self.table is None:
            return None
        if self.variable is None:
            return self.table
        return f"{self.table}#{self.variable}"

    def with_channel(self, channel: CHANNEL) -> CatalogPath:
        """Return new path with different channel."""
        return replace(self, channel=channel)

    def with_namespace(self, namespace: str) -> CatalogPath:
        """Return new path with different namespace."""
        return replace(self, namespace=namespace)

    def with_version(self, version: str) -> CatalogPath:
        """Return new path with different version."""
        return replace(self, version=version)

    def with_dataset(self, dataset: str) -> CatalogPath:
        """Return new path with different dataset."""
        return replace(self, dataset=dataset)

    def with_table(self, table: str | None) -> CatalogPath:
        """Return new path with different table."""
        if table is None:
            return replace(self, table=None, variable=None)
        return replace(self, table=table)

    def with_variable(self, variable: str) -> CatalogPath:
        """Return new path with different variable."""
        if self.table is None:
            raise ValueError("Cannot set variable without table")
        return replace(self, variable=variable)

    def without_variable(self) -> CatalogPath:
        """Return path without variable component."""
        return replace(self, variable=None)

    def without_table(self) -> CatalogPath:
        """Return path without table and variable components."""
        return replace(self, table=None, variable=None)

    @property
    def parent(self) -> CatalogPath:
        """Return parent path (like pathlib.Path.parent).

        - If has variable, returns path without variable
        - If has table (no variable), returns path without table
        - If dataset-only, returns self (dataset is the root)
        """
        if self.variable is not None:
            return replace(self, variable=None)
        if self.table is not None:
            return replace(self, table=None)
        return self

    def __truediv__(self, other: str) -> CatalogPath:
        """Join path components using / operator (like pathlib.Path).

        Examples:
            >>> p = CatalogPath.from_str("grapher/who/2024/gho")
            >>> p / "table"
            CatalogPath('grapher/who/2024/gho/table')
            >>> p / "table#var"
            CatalogPath('grapher/who/2024/gho/table#var')
        """
        if self.table is not None:
            raise ValueError("Cannot append to path that already has a table")

        if "#" in other:
            table, variable = other.split("#", 1)
            return replace(self, table=table, variable=variable)
        return replace(self, table=other)

    def __eq__(self, other: object) -> bool:
        """Compare with another CatalogPath or string."""
        if isinstance(other, str):
            return str(self) == other
        if isinstance(other, CatalogPath):
            return (
                self.channel == other.channel
                and self.namespace == other.namespace
                and self.version == other.version
                and self.dataset == other.dataset
                and self.table == other.table
                and self.variable == other.variable
            )
        return NotImplemented

    def __hash__(self) -> int:
        """Hash for use in sets and dicts."""
        return hash((self.channel, self.namespace, self.version, self.dataset, self.table, self.variable))

    def __fspath__(self) -> str:
        """Return path string for os.fspath() compatibility.

        This allows using CatalogPath directly with pathlib.Path:
            >>> DATA_DIR / catalog_path  # works!
        """
        return self.dataset_path

    def __str__(self) -> str:
        """Full catalog path string."""
        result = self.dataset_path
        if self.table is not None:
            result = f"{result}/{self.table}"
        if self.variable is not None:
            result = f"{result}#{self.variable}"
        return result

    def __repr__(self) -> str:
        """Detailed representation."""
        return f"CatalogPath({str(self)!r})"

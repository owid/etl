"""Read-only dependencies on existing ETL catalog datasets."""

from __future__ import annotations

import pathlib
from dataclasses import dataclass

import pandas as pd
from owid import catalog

from owl.project import load_project


@dataclass(frozen=True)
class ETLDataset:
    """Reference an existing ETL catalog dataset from an Owl step.

    Examples:
        regions = ETLDataset("data://garden/regions/2023-01-01/regions")
        regions = ETLDataset("regions/2023-01-01/regions", channel="garden")

        @Dataset
        def my_dataset(regions: ETLDataset):
            tb = regions.read("regions", reset_index=True)
            ...
    """

    path: str
    channel: str = "garden"

    @property
    def catalog_path(self) -> pathlib.Path:
        """Return the local catalog dataset path."""
        raw = self.path.removeprefix("data://")
        parts = pathlib.PurePosixPath(raw).parts
        if parts and parts[0] in {"garden", "meadow", "grapher"}:
            rel = pathlib.Path(*parts)
        else:
            rel = pathlib.Path(self.channel, *parts)
        return load_project().root / "data" / rel

    def dataset(self) -> catalog.Dataset:
        """Open the underlying owid.catalog Dataset."""
        path = self.catalog_path
        if not (path / "index.json").exists():
            raise FileNotFoundError(f"ETL dataset not found: {path}")
        return catalog.Dataset(path)

    def read(self, table: str | None = None, *, reset_index: bool = True, safe_types: bool = False, **kwargs):
        """Read a table from the ETL dataset.

        If `table` is omitted, the dataset directory name is used, which matches the
        common ETL convention (e.g. regions/2023-01-01/regions -> table 'regions').
        """
        table_name = table or self.catalog_path.name
        return self.dataset().read(table_name, reset_index=reset_index, safe_types=safe_types, **kwargs)

    def load(self) -> pd.DataFrame:
        """Load the default table as a plain pandas DataFrame."""
        return pd.DataFrame(self.read())

    def identity_mtime(self) -> float:
        """Modification time used by Owl stale checks."""
        index = self.catalog_path / "index.json"
        if index.exists():
            return index.stat().st_mtime
        return 0.0

    def __repr__(self) -> str:
        return f"ETLDataset({self.path!r})"

#
#  datasets.py
#
from __future__ import annotations

import hashlib
import json
import shutil
import warnings
from collections.abc import Iterator
from dataclasses import dataclass
from glob import glob
from os import environ
from os.path import join
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import pandas as pd
import yaml

from owid.repack import to_safe_types

from . import tables, utils
from .meta import SOURCE_EXISTS_OPTIONS, DatasetMeta, TableMeta, VariableMeta
from .processing_log import disable_processing_log
from .properties import metadata_property

FileFormat = Literal["csv", "feather", "parquet"]

# the formats we can serialise and deserialise; in some cases they
# will be tried in this order if we don't specify one explicitly
SUPPORTED_FORMATS: list[FileFormat] = ["feather", "parquet", "csv"]

# the formats we generate by default
DEFAULT_FORMATS: list[FileFormat] = environ.get("DEFAULT_FORMATS", "feather").split(",")  # type: ignore

# the format we use by default if we only need one
PREFERRED_FORMAT: FileFormat = "feather"

# sanity checks
assert set(DEFAULT_FORMATS).issubset(SUPPORTED_FORMATS)
assert PREFERRED_FORMAT in DEFAULT_FORMATS
assert SUPPORTED_FORMATS[0] == PREFERRED_FORMAT

# available channels in the catalog
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

# all pandas nullable dtypes
NULLABLE_DTYPES = [f"{sign}{typ}{size}" for typ in ("Int", "Float") for sign in ("", "U") for size in (8, 16, 32, 64)]


@dataclass
class Dataset:
    """
    A dataset is a folder full of data tables, with metadata available at `index.json`.
    """

    path: str
    metadata: "DatasetMeta"

    def __init__(self, path: str | Path) -> None:
        # for convenience, accept Path objects directly
        if isinstance(path, Path):
            self.path = path.as_posix()
        else:
            self.path = path

        self.metadata = DatasetMeta.load(self._index_file)

    @property
    def m(self) -> DatasetMeta:
        """Metadata alias to save typing."""
        return self.metadata

    @classmethod
    def create_empty(cls, path: str | Path, metadata: DatasetMeta | None = None) -> Dataset:
        path = Path(path)

        if path.is_dir():
            if not (path / "index.json").exists():
                raise Exception(f"refuse to overwrite non-dataset dir at: {path}")
            shutil.rmtree(path)

        path.mkdir(parents=True, exist_ok=True)

        metadata = metadata or DatasetMeta()

        index_file = path / "index.json"
        metadata.save(index_file)

        return Dataset(path.as_posix())

    def add(
        self,
        table: tables.Table,
        formats: list[FileFormat] = DEFAULT_FORMATS,
        repack: bool = True,
    ) -> None:
        """
        Add this table to the dataset by saving it in the dataset's folder. By default we
        save in multiple formats, but if you need a specific one (e.g. CSV for explorers)
        you can specify it.

        :param repack: if True, try to cast column types to the smallest possible type (e.g. float64 -> float32)
            to reduce binary file size. Consider using False when your dataframe is large and the repack is failing.
        """

        utils.validate_underscore(table.metadata.short_name, "Table's short_name")
        for col in list(table.columns) + list(table.index.names):
            utils.validate_underscore(col, "Variable's name")

        if not table.primary_key:
            if environ.get("OWID_STRICT"):
                raise PrimaryKeyMissing(
                    f"Table `{table.metadata.short_name}` does not have a primary_key -- please use t.set_index([col, ...], verify_integrity=True) to indicate dimensions before saving"
                )
            else:
                warnings.warn(
                    f"Table `{table.metadata.short_name}` does not have a primary_key -- please use t.set_index([col, ...], verify_integrity=True) to indicate dimensions before saving"
                )

        if not table.index.is_unique and environ.get("OWID_STRICT"):
            [(k, dups)] = table.index.value_counts().head(1).to_dict().items()
            raise NonUniqueIndex(
                f"Table `{table.metadata.short_name}` has duplicate values in the index -- could you have made a mistake?\n\n"
                f"e.g. key {k} is repeated {dups} times in the index"
            )

        # check Float64 and Int64 columns for np.nan
        # see: https://github.com/owid/etl/issues/1334
        for col, dtype in table.dtypes.items():
            if dtype in NULLABLE_DTYPES:
                # pandas nullable types like Float64 have their own pd.NA instead of np.nan
                # make sure we don't use wrong nan, otherwise dropna and other methods won't work
                assert (
                    np.isnan(table[col]).sum() == 0
                ), f"Column `{col}` is using np.nan, but it should be using pd.NA because it has type {table[col].dtype}"

        # copy dataset metadata to the table
        table.metadata.dataset = self.metadata

        for format in formats:
            if format not in SUPPORTED_FORMATS:
                raise Exception(f"Format '{format}'' is not supported")

            table_filename = join(self.path, table.metadata.checked_name + f".{format}")
            table.to(table_filename, repack=repack)

    def read(
        self,
        name: str | None = None,
        reset_index: bool = True,
        safe_types: bool = True,
        reset_metadata: Literal["keep", "keep_origins", "reset"] = "keep",
        load_data: bool = True,
    ) -> tables.Table:
        """Read dataset's table from disk. Alternative to ds[table_name], but
        with more options to optimize the reading.

        :param reset_index: If true, don't set primary keys of the table. This can make loading
            large datasets with multi-indexes much faster.
        :param safe_types: If true, convert numeric columns to Float64 and Int64 and categorical
            columns to string[pyarrow]. This can significantly increase memory usage.
        :param reset_metadata: Controls variable metadata reset behavior.
            - "keep": Leave metadata unchanged (default).
            - "keep_origins": Reset variable metadata but retain the 'origins' attribute.
            - "reset": Reset all variable metadata.
        :param load_data: If false, only load metadata and not the actual data. This can be useful
            when you only need to read metadata from a large dataset.
        """
        if name is None:
            if len(self.table_names) == 1:
                name = self.table_names[0]
            else:
                raise ValueError("Multiple tables exist. Please specify the table name.")
        stem = self.path / Path(name)

        for format in SUPPORTED_FORMATS:
            path = stem.with_suffix(f".{format}")
            if path.exists():
                t = tables.Table.read(path, primary_key=[] if reset_index else None, load_data=load_data)
                t.metadata.dataset = self.metadata
                if safe_types and load_data:
                    t = cast(tables.Table, to_safe_types(t))
                if reset_metadata in ["keep_origins", "reset"]:  # Handles "keep_origins" and "reset"
                    t.metadata = TableMeta()
                    for col in t.columns:
                        if reset_metadata == "keep_origins":  # Preserve 'origins' attribute
                            origins = t[col].metadata.origins if hasattr(t[col].metadata, "origins") else None
                            t[col].metadata = VariableMeta()
                            t[col].metadata.origins = origins  # Preserve 'origins' attribute
                        if reset_metadata == "reset":  # Reset all metadata
                            t[col].metadata = VariableMeta()
                return t

        raise KeyError(f"Table `{name}` not found, available tables: {', '.join(self.table_names)}")

    def __getitem__(self, name: str) -> tables.Table:
        return self.read(name, reset_index=False, safe_types=False)

    def __contains__(self, name: str) -> bool:
        return any((Path(self.path) / name).with_suffix(f".{format}").exists() for format in SUPPORTED_FORMATS)

    def save(self) -> None:
        assert self.metadata.short_name, "Missing dataset short_name"
        utils.validate_underscore(self.metadata.short_name, "Dataset's short_name")

        if not self.metadata.namespace:
            warnings.warn(f"Dataset {self.metadata.short_name} is missing namespace")

        # determine channel automatically from path
        # NOTE: shouldn't we force channel/namespace/version/short_name to be filled from path?
        # see https://github.com/owid/owid-catalog-py/pull/79#issue-1507959097 for discussion
        parts = str(self.path).split("/")
        if len(parts) >= 4:
            channel, _, _, _ = parts[-4:]
            if channel in CHANNEL.__args__:  # type: ignore
                self.metadata.channel = channel

        self.metadata.save(self._index_file)

        # Update the copy of this datasets metadata in every table in the set.
        # TODO: this entire part should go away and we should make t.metadata.dataset read only
        #   also dataset metadata should be only saved in `index.json` and not in every table
        for table_name in self.table_names:
            # NOTE: don't load the table here, that could be slow. Just update the metadata file.
            table_meta_path = Path(self.path) / f"{table_name}.meta.json"

            with open(table_meta_path) as f:
                table_meta = json.load(f)
                table_meta["dataset"] = self.metadata.to_dict()

            with open(table_meta_path, "w") as f:
                json.dump(table_meta, f, indent=2, default=str)

    def update_metadata(
        self,
        metadata_path: Path,
        yaml_params: dict[str, Any] | None = None,
        if_source_exists: SOURCE_EXISTS_OPTIONS = "replace",
        if_origins_exist: SOURCE_EXISTS_OPTIONS = "replace",
        errors: Literal["ignore", "warn", "raise"] = "raise",
        extra_variables: Literal["raise", "ignore"] = "raise",
    ) -> None:
        """
        Load YAML file with metadata from given path and update metadata of dataset and its tables.

        :param metadata_path: Path to *.meta.yml file with metadata. Check out other metadata files
            for examples, this function doesn't do schema validation
        :param yaml_params: Additional parameters to pass to the YAML loader
        :param if_source_exists: What to do if source already exists in metadata. Possible values:
            - "replace" (default): replace existing source with new one
            - "append": append new source to existing ones
            - "fail": raise an exception if source already exists
        :param if_origins_exist: What to do if origin already exists in metadata. Possible values:
            - "replace" (default): replace existing origin with new one
            - "append": append new origin to existing ones
            - "fail": raise an exception if origin already exists
        :param errors: How to handle errors encountered during metadata update. Possible values:
            - "ignore" (default): ignore all errors
            - "warn": issue a warning if there's an indicator in metadata that doesn't exist in the dataset
            - "raise": same as "warn" but also raise an exception
        """
        self.metadata.update_from_yaml(metadata_path, if_source_exists=if_source_exists)

        with open(metadata_path) as istream:
            metadata = yaml.safe_load(istream)
            for table_name in metadata.get("tables", {}).keys():
                with disable_processing_log():
                    try:
                        table = self[table_name]
                    except KeyError as e:
                        if errors == "raise":
                            raise e
                        else:
                            if errors == "warn":
                                warnings.warn(str(e))
                            continue
                table.update_metadata_from_yaml(
                    metadata_path,
                    table_name,
                    if_origins_exist=if_origins_exist,
                    yaml_params=yaml_params,
                    extra_variables=extra_variables,
                )
                table._save_metadata(join(self.path, table.metadata.checked_name + ".meta.json"))

    def index(self, catalog_path: Path = Path("/")) -> pd.DataFrame:
        """
        Return a DataFrame describing the contents of this dataset, one row per table.
        """
        base = {
            "namespace": self.metadata.namespace,
            "dataset": self.metadata.short_name,
            "version": self.metadata.version,
            "checksum": self.checksum(),
            "is_public": self.metadata.is_public,
        }
        rows = []
        for metadata_file in self._metadata_files:
            with open(metadata_file) as istream:
                metadata = TableMeta.from_dict(json.load(istream))

            row = base.copy()

            assert metadata.short_name
            row["table"] = metadata.short_name

            row["dimensions"] = json.dumps(metadata.primary_key)

            table_path = Path(self.path) / metadata.short_name
            relative_path = table_path.relative_to(catalog_path)
            row["path"] = relative_path.as_posix()
            row["channel"] = relative_path.parts[0]

            row["formats"] = [f for f in SUPPORTED_FORMATS if table_path.with_suffix(f".{f}").exists()]  # type: ignore

            rows.append(row)

        return pd.DataFrame.from_records(rows)

    @property
    def _index_file(self) -> str:
        return join(self.path, "index.json")

    def __bool__(self) -> bool:
        return True

    def __len__(self) -> int:
        return len(self.table_names)

    def __iter__(self) -> Iterator[tables.Table]:
        for name in self.table_names:
            yield self[name]

    @property
    def _data_files(self) -> list[str]:
        files = []
        for format in SUPPORTED_FORMATS:
            pattern = join(self.path, f"*.{format}")
            files.extend(glob(pattern))

        return sorted(files)

    @property
    def table_names(self) -> list[str]:
        return sorted({Path(f).stem for f in self._data_files})

    @property
    def _metadata_files(self) -> list[str]:
        return sorted(glob(join(self.path, "*.meta.json")))

    def checksum(self) -> str:
        "Return a MD5 checksum of all data and metadata in the dataset."
        _hash = hashlib.md5()
        _hash.update(checksum_file(self._index_file).digest())

        for data_file in self._data_files:
            _hash.update(checksum_file(data_file).digest())

            metadata_file = Path(data_file).with_suffix(".meta.json").as_posix()
            _hash.update(checksum_file(metadata_file).digest())

        return _hash.hexdigest()


for k in DatasetMeta.__dataclass_fields__:
    if hasattr(Dataset, k):
        raise Exception(f'metadata field "{k}" would overwrite a Dataset built-in')

    setattr(Dataset, k, metadata_property(k))


def checksum_file(filename: str) -> Any:
    "Return the MD5 checksum of a given file."
    chunk_size = 2**20  # 1MB
    checksum = hashlib.md5()
    with open(filename, "rb") as istream:
        chunk = istream.read(chunk_size)
        while chunk:
            checksum.update(chunk)
            chunk = istream.read(chunk_size)

    return checksum


class PrimaryKeyMissing(Exception):
    pass


class NonUniqueIndex(Exception):
    pass

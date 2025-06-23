#
#  tables.py
#
from __future__ import annotations

import json
import time
import types
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator
from functools import wraps
from os.path import dirname, join, splitext
from pathlib import Path
from typing import (
    IO,
    Any,
    Literal,
    TypeVar,
    cast,
    overload,
)

import numpy as np
import pandas as pd
import pyarrow
import pyarrow.parquet as pq
import rdata
import structlog
from pandas._typing import FilePath, ReadCsvBuffer, Scalar  # type: ignore
from pandas.core.series import Series

from owid.datautils import dataframes
from owid.repack import repack_frame

from . import processing_log as pl
from . import utils, variables, warnings
from .meta import SOURCE_EXISTS_OPTIONS, DatasetMeta, License, Origin, Source, TableMeta, VariableMeta

log = structlog.get_logger()

SCHEMA = json.load(open(join(dirname(__file__), "schemas", "table.json")))
METADATA_FIELDS = list(SCHEMA["properties"])

# New type required for pandas reading functions.
AnyStr = TypeVar("AnyStr", str, bytes)

# pd.Series or Variable
SeriesOrVariable = TypeVar("SeriesOrVariable", pd.Series, variables.Variable)


class Table(pd.DataFrame):
    # metdata about the entire table
    metadata: TableMeta

    # metadata about individual columns
    # NOTE: the name _fields is also on the Variable class, pandas will propagate this to
    #       any slices, which is how they get access to their metadata
    _fields: dict[str, VariableMeta]

    # propagate all these fields on every slice or copy
    _metadata = ["metadata", "_fields"]

    # Set to True to help debugging metadata issues.
    DEBUG = False

    # slicing and copying creates tables
    @property
    def _constructor(self) -> type:
        return Table

    @property
    def _constructor_sliced(self) -> Any:
        return variables.Variable

    def __init__(
        self,
        *args: Any,
        metadata: TableMeta | None = None,
        short_name: str | None = None,
        underscore=False,
        camel_to_snake=False,
        like: Table | None = None,
        **kwargs: Any,
    ) -> None:
        """
        :param metadata: TableMeta to use
        :param short_name: Use empty TableMeta and fill it with `short_name`. This is a shorter version
            of `Table(df, metadata=TableMeta(short_name="my_name"))`
        :param underscore: Underscore table columns and indexes. See `underscore` method for help
        :param camel_to_snake: Convert camelCase column names to snake_case.
        :param like: Use metadata from Table given in this argument (including columns). This is a shorter version of
            new_t = Table(df, metadata=old_t.metadata)
            for col in new_t.columns:
                new_t[col].metadata = deepcopy(old_t[col].metadata)
        """

        super().__init__(*args, **kwargs)

        # empty table metadata by default
        self.metadata = metadata or TableMeta()

        # use supplied short_name
        if short_name:
            assert self.metadata.short_name is None or (
                self.metadata.short_name == short_name
            ), "short_name is different from the one in metadata"
            self.metadata.short_name = short_name

        # all columns have empty metadata by default
        assert not hasattr(self, "_fields")
        self._fields = defaultdict(VariableMeta)

        # underscore column names
        if underscore:
            self.underscore(inplace=True, camel_to_snake=camel_to_snake)

        # reuse metadata from a different table
        if like is not None:
            copy = self.copy_metadata(like)
            self._fields = copy._fields
            self.metadata = copy.metadata

    @property
    def m(self) -> TableMeta:
        """Metadata alias to save typing."""
        return self.metadata

    @property
    def primary_key(self) -> list[str]:
        return [n for n in self.index.names if n]

    def to(self, path: str | Path, repack: bool = True) -> None:
        """
        Save this table in one of our SUPPORTED_FORMATS.
        """
        # Add entry in the processing log about operation "save".
        self = update_processing_logs_when_saving_table(table=self, path=path)

        if isinstance(path, Path):
            path = path.as_posix()

        if path.endswith(".csv"):
            # ignore repacking
            return self.to_csv(path)

        elif path.endswith(".feather"):
            return self.to_feather(path, repack=repack)

        elif path.endswith(".parquet"):
            return self.to_parquet(path, repack=repack)

        else:
            raise ValueError(f"could not detect a suitable format to save to: {path}")

    @classmethod
    def read(cls, path: str | Path, **kwargs) -> Table:
        if isinstance(path, Path):
            path = path.as_posix()

        if path.endswith(".csv"):
            table = cls.read_csv(path, **kwargs)

        elif path.endswith(".feather"):
            table = cls.read_feather(path, **kwargs)

        elif path.endswith(".parquet"):
            table = cls.read_parquet(path, **kwargs)
        else:
            raise ValueError(f"could not detect a suitable format to read from: {path}")

        # Add processing log to the metadata of each variable in the table.
        table = update_processing_logs_when_loading_or_creating_table(table=table)

        # Fill dimensions from additional_info for compatibility
        for col in table.columns:
            dims = (table[col].m.additional_info or {}).get("dimensions")
            if dims:
                update_variable_dimensions(table[col], dims)

        if cls.DEBUG:
            table.check_metadata()

        return table

    @overload
    def to_csv(self, path: None = None, **kwargs: Any) -> str: ...

    @overload
    def to_csv(self, path: Any, **kwargs: Any) -> None: ...

    def to_csv(self, path: Any | None = None, **kwargs: Any) -> None | str:
        """
        Save this table as a csv file plus accompanying JSON metadata file.
        If the table is stored at "mytable.csv", the metadata will be at
        "mytable.meta.json".
        """
        # return string
        if path is None:
            return super().to_csv(**kwargs)

        if not str(path).endswith(".csv"):
            raise ValueError(f'filename must end in ".csv": {path}')

        df = pd.DataFrame(self)
        if "index" not in kwargs:
            # if the dataframe uses the default index then we don't want to store it (would be a column of row numbers)
            # NOTE: By default pandas does store the index, and users often explicitly add "index=False".
            kwargs["index"] = self.primary_key != []
        df.to_csv(path, **kwargs)

        metadata_filename = splitext(path)[0] + ".meta.json"
        self._save_metadata(metadata_filename)

    @property
    def codebook(self) -> pd.DataFrame:
        """
        Return a codebook for this table.
        """

        # Define how to show attributions and URLs in the sources column.
        def _prepare_attributions(attribution: str, url_main: str) -> str:
            return f"{attribution} ( {url_main} )"

        # Initialize lists to store the codebook information.
        columns = []
        titles = []
        descriptions = []
        sources = []
        for column in self.columns:
            md = self[column].metadata
            columns.append(column)
            titles.append(getattr(md.presentation, "title_public", None) or md.title)
            # Use short description (after removing details on demand, if any).
            descriptions.append(utils.remove_details_on_demand(md.description_short))
            sources.append(
                "; ".join(
                    dict.fromkeys(
                        _prepare_attributions(
                            origin.attribution if origin.attribution else origin.producer, origin.url_main
                        )
                        for origin in md.origins
                    )
                )
            )

        # Create a DataFrame with the codebook.
        codebook = pd.DataFrame({"column": columns, "title": titles, "description": descriptions, "sources": sources})

        return codebook

    def to_excel(
        self,
        excel_writer: Any,
        with_metadata=True,
        sheet_name="data",
        metadata_sheet_name="metadata",
        **kwargs: Any,
    ) -> None:
        # Save data and codebook to an excel file.
        with pd.ExcelWriter(excel_writer) as writer:  # type: ignore
            super().to_excel(writer, sheet_name=sheet_name, **kwargs)
            if with_metadata:
                self.codebook.to_excel(writer, sheet_name=metadata_sheet_name)

    def to_feather(
        self,
        path: Any,
        repack: bool = True,
        compression: Literal["zstd", "lz4", "uncompressed"] = "zstd",
        **kwargs: Any,
    ) -> None:
        """
        Save this table as a feather file plus accompanying JSON metadata file.
        If the table is stored at "mytable.feather", the metadata will be at
        "mytable.meta.json".
        """
        if not str(path).endswith(".feather"):
            raise ValueError(f'filename must end in ".feather": {path}')

        # feather can't store the index
        df = pd.DataFrame(self)
        if self.primary_key:
            overlapping_names = set(self.index.names) & set(self.columns)
            if overlapping_names:
                raise ValueError(f"index names are overlapping with column names: {overlapping_names}")
            df = df.reset_index()

        if repack:
            # use smaller data types wherever possible
            # NOTE: this can be slow for large dataframes
            t = time.time()
            df = repack_frame(df)
            if time.time() - t > 5:
                log.warning(
                    "repacking took a long time, consider adding create_dataset(..., repack=False)",
                    time=time.time() - t,
                )

        df.to_feather(path, compression=compression, **kwargs)

        self._save_metadata(self.metadata_filename(path))

    def metadata_filename(self, path: str):
        return splitext(path)[0] + ".meta.json"

    def to_parquet(self, path: Any, repack: bool = True) -> None:  # type: ignore
        """
        Save this table as a parquet file with embedded metadata in the table schema.

        NOTE: we save the metadata for fields in the table scheme, but it might be
              possible with Parquet to store it in the fields themselves somehow
        """
        if not str(path).endswith(".parquet"):
            raise ValueError(f'filename must end in ".parquet": {path}')

        # parquet can store the index, but repacking is wasted on index columns so
        # we get rid of the index first
        df = pd.DataFrame(self)
        if self.primary_key:
            df = df.reset_index()

        if repack:
            # use smaller data types wherever possible
            # NOTE: this can be slow for large dataframes
            df = repack_frame(df)

        # create a pyarrow table with metadata in the schema
        # (some metadata gets auto-generated to help pandas deserialise better, we want to keep that)
        t = pyarrow.Table.from_pandas(df)

        # adding metadata would make reading partial content inefficient, see https://github.com/owid/etl/issues/783
        # new_metadata = {
        #     b"owid_table": json.dumps(self.metadata.to_dict(), default=str),  # type: ignore
        #     b"owid_fields": json.dumps(self._get_fields_as_dict(), default=str),
        #     b"primary_key": json.dumps(self.primary_key),
        #     **t.schema.metadata,
        # }
        # schema = t.schema.with_metadata(new_metadata)
        # t = t.cast(schema)

        # write the combined table to disk
        pq.write_table(t, path)

        self._save_metadata(self.metadata_filename(path))

    def _save_metadata(self, filename: str) -> None:
        # write metadata
        with open(filename, "w") as ostream:
            metadata = self.metadata.to_dict()  # type: ignore
            metadata["primary_key"] = self.primary_key
            metadata["fields"] = self._get_fields_as_dict()
            try:
                json.dump(metadata, ostream, indent=2, default=str, allow_nan=False)
            except ValueError as e:
                # try to find a problematic field
                for k, v in metadata["fields"].items():
                    try:
                        json.dumps(v, default=str, allow_nan=False)
                    except ValueError as e2:
                        raise ValueError(f"metadata field {k} contains NaNs:\n{v}") from e2
                raise ValueError(f"metadata contains NaNs:\n{metadata}") from e

    @classmethod
    def read_csv(cls, path: str | Path, **kwargs) -> Table:
        """
        Read the table from csv plus accompanying JSON sidecar.
        """
        if isinstance(path, Path):
            path = path.as_posix()

        if not path.endswith(".csv"):
            raise ValueError(f'filename must end in ".csv": {path}')

        # load the data and add metadata
        tb = Table(pd.read_csv(path, index_col=False, na_values=[""], keep_default_na=False))
        cls._add_metadata(tb, path, **kwargs)
        return tb

    def update_metadata(self, **kwargs) -> Table:
        """Set Table metadata."""
        for k, v in kwargs.items():
            assert hasattr(self.metadata, k), f"unknown metadata field {k} in TableMeta"
            setattr(self.metadata, k, v)
        return self

    @classmethod
    def _add_metadata(cls, tb: Table, path: str, primary_key: list[str] | None = None, load_data: bool = True) -> None:
        """Read metadata from JSON sidecar and add it to the dataframe."""
        if not load_data:
            log.warning("Using load_data=False is only supported when reading feather format.")

        metadata = cls._read_metadata(path)

        if primary_key is None:
            primary_key = metadata.get("primary_key", [])
        fields = metadata.pop("fields") if "fields" in metadata else {}

        tb.metadata = TableMeta.from_dict(metadata)
        tb._set_fields_from_dict(fields)

        # NOTE: setting index is really slow for large datasets
        if primary_key:
            tb.set_index(primary_key, inplace=True)

    @classmethod
    def read_feather(cls, path: str | Path, load_data: bool = True, **kwargs) -> Table:
        """
        Read the table from feather plus accompanying JSON sidecar.

        The path may be a local file path or a URL.
        """
        if isinstance(path, Path):
            path = path.as_posix()

        if not path.endswith(".feather"):
            raise ValueError(f'filename must end in ".feather": {path}')

        # load the data and add metadata
        if not load_data:
            metadata = cls._read_metadata(path)
            columns = list(metadata["fields"].keys())
            df = Table(pd.DataFrame(columns=columns))
        else:
            df = Table(pd.read_feather(path))

        cls._add_metadata(df, path, **kwargs)
        return df

    @classmethod
    def read_parquet(cls, path: str | Path, **kwargs) -> Table:
        """
        Read the table from a parquet file plus accompanying JSON sidecar.

        The path may be a local file path or a URL.
        """
        if isinstance(path, Path):
            path = path.as_posix()

        if not path.endswith(".parquet"):
            raise ValueError(f'filename must end in ".parquet": {path}')

        # load the data and add metadata
        df = Table(pd.read_parquet(path))
        cls._add_metadata(df, path, **kwargs)
        return df

    def _get_fields_as_dict(self) -> dict[str, Any]:
        return {col: self._fields[col].to_dict() for col in self.all_columns}

    def _set_fields_from_dict(self, fields: dict[str, Any]) -> None:
        self._fields = defaultdict(VariableMeta, {k: VariableMeta.from_dict(v) for k, v in fields.items()})

    @staticmethod
    def _read_metadata(data_path: str) -> dict[str, Any]:
        import requests

        metadata_path = splitext(data_path)[0] + ".meta.json"

        if metadata_path.startswith("http"):
            return cast(dict[str, Any], requests.get(metadata_path).json())

        with open(metadata_path) as istream:
            return cast(dict[str, Any], json.load(istream))

    def __setitem__(self, key: Any, value: Any) -> Any:
        super().__setitem__(key, value)

        # propagate metadata when we add a series to a table
        if isinstance(key, str):
            if isinstance(value, variables.Variable):
                # variable needs to be assigned name to make VariableMeta work
                if not value.name:
                    value.name = key
                if value.name == variables.UNNAMED_VARIABLE:
                    # Update the variable name, if it had the unnamed variable tag.
                    # Replace all instances of unnamed variables in the processing log by the actual name of the new
                    # variable.
                    # WARNING: This process assumes that all instances of unnamed variable tag correspond to the new
                    #  variable.
                    value.name = key
                self._fields[key] = value.metadata
                value.update_log(operation="rename", variable=key)
            # assign Table with a single column should work
            elif isinstance(value, Table) and value.shape[1] == 1:
                self._fields[key] = value.iloc[:, 0].metadata
            else:
                self._fields[key] = VariableMeta()

        if self.DEBUG:
            self.check_metadata()

    def equals_table(self, table: Table) -> bool:
        return (
            isinstance(table, Table)
            and self.metadata == table.metadata
            # By simply doing self.to_dict() == table.to_dict() may return False if the dictionaries are identical but
            # contain nans.
            and self.fillna(123456).to_dict() == table.fillna(123456).to_dict()
            and self._fields == table._fields
        )

    @overload
    def rename(
        self,
        mapper: Any = None,
        *,
        inplace: Literal[True],
        **kwargs: Any,
    ) -> None: ...

    @overload
    def rename(self, mapper: Any = None, *, inplace: Literal[False], **kwargs: Any) -> Table: ...

    @overload
    def rename(self, *args: Any, **kwargs: Any) -> Table: ...

    def rename(self, *args: Any, **kwargs: Any) -> Table | None:
        """Rename columns while keeping their metadata."""
        inplace = kwargs.get("inplace")
        old_cols = self.all_columns
        new_table = super().rename(*args, **kwargs)

        # __setattr__ on columns has already done its job of renaming
        if inplace:
            new_table = self
        else:
            assert new_table is not None
            # construct new _fields attribute
            fields = {}
            for old_col, new_col in zip(old_cols, new_table.all_columns):
                fields[new_col] = self._fields[old_col].copy()

            new_table._fields = defaultdict(VariableMeta, fields)

        for old_col, new_col in zip(old_cols, new_table.all_columns):
            # Update processing log.
            if old_col != new_col:
                new_table._fields[new_col].processing_log.add_entry(
                    variable=new_col,
                    parents=[self._fields[old_col]],
                    operation="rename",
                )

        if inplace:
            return None
        else:
            return cast(Table, new_table)

    def __setattr__(self, name: str, value) -> None:
        # setting columns must rename them
        if name == "columns":
            for old_col, new_col in zip(self.columns, value):
                if old_col in self._fields:
                    self._fields[new_col] = self._fields.pop(old_col)

        super().__setattr__(name, value)

    @property
    def all_columns(self) -> list[str]:
        "Return names of all columns in the dataset, including the index."
        combined: list[str] = filter(None, list(self.index.names) + list(self.columns))  # type: ignore
        return combined

    def get_column_or_index(self, name) -> variables.Variable:
        if name in self.columns:
            return self[name]
        elif name in self.index.names:
            return variables.Variable(self.index.get_level_values(name), name=name, metadata=self._fields[name])
        else:
            raise ValueError(f"'{name}' not found in columns or index")

    def update_metadata_from_yaml(
        self,
        path: Path | str,
        table_name: str,
        yaml_params: dict[str, Any] | None = None,
        extra_variables: Literal["raise", "ignore"] = "raise",
        if_origins_exist: SOURCE_EXISTS_OPTIONS = "replace",
    ) -> None:
        """Update metadata of table and variables from a YAML file.
        :param path: Path to YAML file.
        :param table_name: Name of table, also updates this in the metadata.
        """
        from .yaml_metadata import update_metadata_from_yaml

        return update_metadata_from_yaml(
            tb=self,
            path=path,
            table_name=table_name,
            extra_variables=extra_variables,
            yaml_params=yaml_params,
            if_origins_exist=if_origins_exist,
        )

    def prune_metadata(self) -> Table:
        """Prune metadata for columns that are not in the table. This can happen after slicing
        the table by columns."""
        self._fields = defaultdict(VariableMeta, {col: self._fields[col] for col in self.all_columns})
        return self

    def copy(self, deep: bool = True) -> Table:
        """Copy table together with all its metadata."""
        # This could be causing this warning:
        #   Passing a BlockManager to Table is deprecated and will raise in a future version. Use public APIs instead.
        # but I'm not sure how to fix it
        tab = super().copy(deep=deep)
        return tab.copy_metadata(self)

    def copy_metadata(self, from_table: Table, deep: bool = False) -> Table:
        """Copy metadata from a different table to self."""
        return copy_metadata(to_table=self, from_table=from_table, deep=deep)

    @overload
    def set_index(
        self,
        keys: str | list[str],
        *,
        inplace: Literal[True],
        **kwargs: Any,
    ) -> None: ...

    @overload
    def set_index(self, keys: str | list[str], *, inplace: Literal[False], **kwargs: Any) -> Table: ...

    @overload
    def set_index(self, keys: str | list[str], **kwargs: Any) -> Table: ...

    def set_index(
        self,
        keys: str | list[str],
        **kwargs: Any,
    ) -> Table | None:
        if isinstance(keys, str):
            keys = [keys]

        # create metadata dimensions
        for col in keys:
            # TODO: make this work with append=True
            dimensions = [{"name": self[col].title or key, "slug": key} for key in keys]

        if kwargs.get("inplace"):
            super().set_index(keys, **kwargs)
            t = self
            to_return = None
        else:
            t = super().set_index(keys, **kwargs)
            to_return = cast(Table, t)

        t.metadata.primary_key = keys
        t.metadata.dimensions = dimensions  # type: ignore
        return to_return

    @overload
    def reset_index(self, level=None, *, inplace: Literal[True], **kwargs) -> None: ...

    @overload
    def reset_index(self, level=None, *, inplace: Literal[False], **kwargs) -> Table: ...

    @overload
    def reset_index(self, level=None, *, inplace: bool = False, **kwargs) -> Table: ...

    def reset_index(self, level=None, *, inplace: bool = False, **kwargs) -> Table | None:  # type: ignore
        """Fix type signature of reset_index."""
        t = super().reset_index(level=level, inplace=inplace, **kwargs)  # type: ignore

        if inplace:
            # TODO: make this work for reset_index with subset of levels
            # drop dimensions
            self.metadata.dimensions = None
            return None
        else:
            # preserve metadata in _fields, calling reset_index() on a table drops it
            t._fields = self._fields
            # drop dimensions
            t.metadata.dimensions = None
            return t  # type: ignore

    def astype(self, *args, **kwargs) -> Table:
        return super().astype(*args, **kwargs)  # type: ignore

    def reindex(self, *args, **kwargs) -> Table:
        t = super().reindex(*args, **kwargs)
        return cast(Table, t)

    @overload
    def drop_duplicates(self, *, inplace: Literal[True], **kwargs) -> None: ...

    @overload
    def drop_duplicates(self, *, inplace: Literal[False], **kwargs) -> Table: ...

    @overload
    def drop_duplicates(self, **kwargs) -> Table: ...

    def drop_duplicates(self, *args, **kwargs) -> Table | None:
        return super().drop_duplicates(*args, **kwargs)

    def join(self, other: pd.DataFrame | Table, *args, **kwargs) -> Table:
        """Fix type signature of join."""
        t = super().join(other, *args, **kwargs)

        t = t.copy_metadata(self)

        # copy variables metadata from other table
        if isinstance(other, Table):
            for k, v in other._fields.items():
                t._fields[k] = v.copy()
        return t  # type: ignore

    def _repr_html_(self):
        html = super()._repr_html_()  # type: ignore
        if self.DEBUG:
            self.check_metadata()
        return f"""
             <h2 style="margin-bottom: 0em"><pre>{self.metadata.short_name}</pre></h2>
             <p style="font-variant: small-caps; font-size: 1.5em; font-family: sans-serif; color: grey; margin-top: -0.2em; margin-bottom: 0.2em">table</p>
             {html}
        """

    def merge(self, right, *args, **kwargs) -> Table:
        return merge(left=self, right=right, *args, **kwargs)

    def melt(
        self,
        id_vars: tuple[str] | list[str] | str | None = None,
        value_vars: tuple[str] | list[str] | str | None = None,
        var_name: str = "variable",
        value_name: str = "value",
        short_name: str | None = None,
        *args,
        **kwargs,
    ) -> Table:
        return melt(
            frame=self,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name,
            short_name=short_name,
            *args,
            **kwargs,
        )

    def pivot(
        self,
        *,
        index: str | list[str] | None = None,
        columns: str | list[str] | None = None,
        values: str | list[str] | None = None,
        join_column_levels_with: str | None = None,
        short_name: str | None = None,
        fill_dimensions: bool = True,
        **kwargs,
    ) -> Table:
        return pivot(
            data=self,
            index=index,
            columns=columns,
            values=values,
            join_column_levels_with=join_column_levels_with,
            short_name=short_name,
            fill_dimensions=fill_dimensions,
            **kwargs,
        )

    def underscore(
        self,
        collision: Literal["raise", "rename", "ignore"] = "raise",
        inplace: bool = False,
        camel_to_snake: bool = False,
    ) -> Table:
        """Convert column and index names to underscore. In extremely rare cases
        two columns might have the same underscored version. Use `collision` param
        to control whether to raise an error or append numbered suffix.

        Parameters
        ----------
        t : Table
            Table to underscore.
        collision : Literal["raise", "rename", "ignore"], optional
            How to handle collisions, by default "raise".
        inplace : bool, optional
            Whether to modify the table in place, by default False.
        camel_to_snake : bool, optional
            Whether to convert strings camelCase to snake_case, by default False.
        """
        t = self
        orig_cols = t.columns

        # underscore columns and resolve collisions
        new_cols = pd.Index([utils.underscore(c, camel_to_snake=camel_to_snake) for c in t.columns])
        new_cols = _resolve_collisions(orig_cols, new_cols, collision)

        columns_map = {c_old: c_new for c_old, c_new in zip(orig_cols, new_cols)}
        if inplace:
            t.rename(columns=columns_map, inplace=True)
        else:
            t = t.rename(columns=columns_map)

        t.index.names = [utils.underscore(e, camel_to_snake=camel_to_snake) for e in t.index.names]
        t.metadata.primary_key = t.primary_key
        t.metadata.short_name = utils.underscore(t.metadata.short_name, camel_to_snake=camel_to_snake)

        # put original names as titles into metadata by default
        for c_old, c_new in columns_map.items():
            # if underscoring didn't change anything, don't add title
            if t[c_new].metadata.title is None and c_old != c_new:
                t[c_new].metadata.title = c_old

        return t

    def format(
        self,
        keys: str | list[str] | None = None,
        verify_integrity: bool = True,
        underscore: bool = True,
        sort_rows: bool = True,
        sort_columns: bool = False,
        short_name: str | None = None,
        **kwargs,
    ) -> Table:
        """Format the table according to OWID standards.

        This includes underscoring column names, setting index, verifying there is only one entry per index, sorting by index.

        Underscoring is the first step, so make sure to use correct values in `keys` (e.g. use 'country' if original table had 'Country').

        ```
        tb.format(["country", "year"])
        ```

        is equivalent to

        ```
        tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()
        ```

        NOTE: You can use default `tb.format()`, which uses keys = ['country', 'year'].

        Parameters
        ----------
        keys : str | list[str] | None, optional
            Index columns. If none is given, will use ["country", "year"].
        verify_integrity : bool, optional
            Verify that there is only one entry per index, by default True.
        underscore : bool, optional
            Underscore column names, by default True.
        sort_rows : bool, optional
            Sort rows by index (ascending), by default True.
        sort_columns : bool, optional
            Sort columns (ascending), by default False.
        short_name : str | None, optional
            Short name to assign to the output table.
        kwargs : Any
            Passed to `Table.underscore` method.
        """
        t = self
        # Underscore
        if underscore:
            t = t.underscore(**kwargs)
        # Set index
        if keys is None:
            keys = ["country", "year"]
        # Underscore keys
        elif isinstance(keys, str):
            keys = utils.underscore(keys)
        else:
            keys = [utils.underscore(k) for k in keys]
        ## Sanity check
        try:
            t = t.set_index(keys, verify_integrity=verify_integrity)
        except KeyError as e:
            if underscore:
                raise KeyError(
                    f"Make sure that you are using valid column names! Note that the column names have been underscored! Available column names are: {t.columns}. You used {keys}."
                )
            else:
                raise e
        if sort_columns:
            t = t.sort_index(axis=1)
        # Sort rows
        if sort_rows:
            t = t.sort_index(axis=0)
        # Rename table.
        if short_name:
            t.metadata.short_name = short_name

        return t

    @overload
    def dropna(self, *, inplace: Literal[True], **kwargs) -> None: ...

    @overload
    def dropna(self, *, inplace: Literal[False], **kwargs) -> Table: ...

    @overload
    def dropna(self, **kwargs) -> Table: ...

    def dropna(self, *args, **kwargs) -> Table | None:
        tb = super().dropna(*args, **kwargs)
        # inplace returns None
        if tb is None:
            return None
        tb = tb.copy()
        for column in list(tb.all_columns):
            tb._fields[column].processing_log.add_entry(
                variable=column,
                parents=[tb.get_column_or_index(column)],
                operation="dropna",
            )

        return cast(Table, tb)

    def drop(self, *args, **kwargs) -> Table:
        return cast(Table, super().drop(*args, **kwargs))

    def filter(self, *args, **kwargs) -> Table:
        return super().filter(*args, **kwargs)  # type: ignore

    def update_log(
        self,
        operation: str,
        parents: list[Any] | None = None,
        variable_names: list[str] | None = None,
        comment: str | None = None,
    ) -> Table:
        # Append a new entry to the processing log of the required variables.
        if variable_names is None:
            # If no variable is specified, assume all (including index columns).
            variable_names = list(self.all_columns)
        for column in variable_names:
            # If parents is not defined, assume the parents are simply the current variable.
            _parents = parents or [column]
            # Update (in place) the processing log of current variable.
            self._fields[column].processing_log.add_entry(
                variable=column,
                parents=_parents,
                operation=operation,
                comment=comment,
            )
        return self

    def amend_log(
        self,
        variable_names: list[str] | None = None,
        comment: str | None = None,
        operation: str | None = None,
    ) -> Table:
        """Amend operation or comment of the latest processing log entry."""
        # Append a new entry to the processing log of the required variables.
        if variable_names is None:
            # If no variable is specified, assume all (including index columns).
            variable_names = list(self.all_columns)
        for column in variable_names:
            # Update (in place) the processing log of current variable.
            self._fields[column].processing_log.amend_entry(
                operation=operation,
                comment=comment,
            )
        return self

    def sort_values(self, by: str | list[str], *args, **kwargs) -> Table:
        tb = super().sort_values(by=by, *args, **kwargs).copy()
        for column in list(tb.all_columns):
            if isinstance(by, str):
                parents = [by, column]
            else:
                parents = by + [column]

            parent_variables = [tb.get_column_or_index(parent) for parent in parents]

            tb._fields[column].processing_log.add_entry(variable=column, parents=parent_variables, operation="sort")

        return cast(Table, tb)

    def sum(self, *args, **kwargs) -> variables.Variable:
        variable_name = variables.UNNAMED_VARIABLE
        variable = variables.Variable(super().sum(*args, **kwargs), name=variable_name)  # type: ignore
        variable.metadata = variables.combine_variables_metadata(
            variables=[self[column] for column in self.columns], operation="+", name=variable_name
        )

        return variable

    def prod(self, *args, **kwargs) -> variables.Variable:
        variable_name = variables.UNNAMED_VARIABLE
        variable = variables.Variable(super().prod(*args, **kwargs), name=variable_name)  # type: ignore
        variable.metadata = variables.combine_variables_metadata(
            variables=[self[column] for column in self.columns], operation="*", name=variable_name
        )

        return variable

    def assign(self, *args, **kwargs) -> Table:
        return super().assign(*args, **kwargs)  # type: ignore

    def reorder_levels(self, *args, **kwargs) -> Table:
        return super().reorder_levels(*args, **kwargs)  # type: ignore

    @staticmethod
    def _update_log(tb: Table, other: Scalar | Series | variables.Variable | Table, operation: str) -> None:  # type: ignore
        # The following would have a parents only the scalar, not the scalar and the corresponding variable.
        # tb = update_log(table=tb, operation="+", parents=[other], variable_names=tb.columns)
        # Instead, update the processing log of each variable in the table.
        for column in tb.columns:
            if isinstance(other, pd.DataFrame):
                parents = [tb[column], other[column]]
            else:
                parents = [tb[column], other]
            tb[column].update_log(parents=parents, operation=operation)

    def __add__(self, other: Scalar | Series | variables.Variable | Table) -> Table:  # type: ignore
        tb = cast(Table, Table(super().__add__(other=other)).copy_metadata(self))
        self._update_log(tb, other, "+")
        return tb

    def __iadd__(self, other: Scalar | Series | variables.Variable | Table) -> Table:  # type: ignore
        return self.__add__(other)

    def __sub__(self, other: Scalar | Series | variables.Variable | Table) -> Table:  # type: ignore
        tb = cast(Table, Table(super().__sub__(other=other)).copy_metadata(self))
        self._update_log(tb, other, "-")
        return tb

    def __isub__(self, other: Scalar | Series | variables.Variable | Table) -> Table:  # type: ignore
        return self.__sub__(other)

    def __mul__(self, other: Scalar | Series | variables.Variable | Table) -> Table:  # type: ignore
        tb = cast(Table, Table(super().__mul__(other=other)).copy_metadata(self))
        self._update_log(tb, other, "*")
        return tb

    def __imul__(self, other: Scalar | Series | variables.Variable | Table) -> Table:  # type: ignore
        return self.__mul__(other)

    def __truediv__(self, other: Scalar | Series | variables.Variable | Table) -> Table:  # type: ignore
        tb = cast(Table, Table(super().__truediv__(other=other)).copy_metadata(self))
        self._update_log(tb, other, "/")
        return tb

    def __itruediv__(self, other: Scalar | Series | variables.Variable | Table) -> Table:  # type: ignore
        return self.__truediv__(other)

    def __floordiv__(self, other: Scalar | Series | variables.Variable | Table) -> Table:  # type: ignore
        tb = cast(Table, Table(super().__floordiv__(other=other)).copy_metadata(self))
        self._update_log(tb, other, "//")
        return tb

    def __ifloordiv__(self, other: Scalar | Series | variables.Variable | Table) -> Table:  # type: ignore
        return self.__floordiv__(other)

    def __mod__(self, other: Scalar | Series | variables.Variable | Table) -> Table:  # type: ignore
        tb = cast(Table, Table(super().__mod__(other=other)).copy_metadata(self))
        self._update_log(tb, other, "%")
        return tb

    def __imod__(self, other: Scalar | Series | variables.Variable | Table) -> Table:  # type: ignore
        return self.__mod__(other)

    def __pow__(self, other: Scalar | Series | variables.Variable | Table) -> Table:  # type: ignore
        tb = cast(Table, Table(super().__pow__(other=other)).copy_metadata(self))
        self._update_log(tb, other, "**")
        return tb

    def __ipow__(self, other: Scalar | Series | variables.Variable | Table) -> Table:  # type: ignore
        return self.__pow__(other)

    def sort_index(self, *args, **kwargs) -> Table:
        return super().sort_index(*args, **kwargs)  # type: ignore

    def groupby(self, *args, observed=True, **kwargs) -> "TableGroupBy":
        """Groupby that preserves metadata. It uses observed=True by default."""
        return TableGroupBy(
            pd.DataFrame.groupby(self.copy(deep=False), *args, observed=observed, **kwargs), self.metadata, self._fields
        )

    def rolling(self, *args, **kwargs) -> "TableRolling":
        """Rolling operation that preserves metadata."""
        return TableRolling(super().rolling(*args, **kwargs), self.metadata, self._fields)  # type: ignore

    def check_metadata(self, ignore_columns: list[str] | None = None) -> None:
        """Check that all variables in the table have origins."""
        if ignore_columns is None:
            if self.primary_key:
                ignore_columns = self.primary_key
            else:
                ignore_columns = ["year", "country"]

        for column in [column for column in self.columns if column not in ignore_columns]:
            if not self[column].metadata.origins:
                warnings.warn(f"Variable {column} has no origins.", warnings.NoOriginsWarning)

    def rename_index_names(self, renames: dict[str, str]) -> Table:
        """Rename index."""
        column_idx = list(self.index.names)
        column_idx_new = [renames.get(col, col) for col in column_idx]
        tb = self.reset_index().rename(columns=renames)
        tb = tb.set_index(column_idx_new)
        return tb

    def fillna(self, value=None, **kwargs) -> Table:
        """Usual fillna, but, if the object given to fill values with is a table, transfer its metadata to the filled
        table."""
        if value is not None:
            tb = super().fillna(value, **kwargs)

            if type(value) is type(self):
                for column in tb.columns:
                    if column in value.columns:
                        tb._fields[column] = variables.combine_variables_metadata(
                            variables=[tb[column], value[column]], operation="fillna", name=column
                        )
        else:
            tb = super().fillna(**kwargs)

        tb = cast(Table, tb)
        return tb

    @classmethod
    def from_records(cls, *args, **kwargs) -> Table:
        """Calling Table.from_records returns a Table, but does not call __init__ and misses metadata."""
        df = super().from_records(*args, **kwargs)
        return Table(df)


def _create_table(df: pd.DataFrame, metadata: TableMeta, fields: dict[str, VariableMeta]) -> Table:
    """Create a table with metadata."""
    tb = Table(df, metadata=metadata.copy())
    tb._fields = defaultdict(VariableMeta, fields)
    return tb


class TableGroupBy:
    # fixes type hints
    __annotations__ = {}

    def __init__(self, groupby: pd.core.groupby.DataFrameGroupBy, metadata: TableMeta, fields: dict[str, Any]):
        self.groupby = groupby
        self.metadata = metadata
        self._fields = fields

    @overload
    def __getattr__(self, name: Literal["count", "size", "sum", "mean", "median"]) -> Callable[[], Table]: ...

    @overload
    def __getattr__(self, name: str) -> VariableGroupBy: ...

    def __getattr__(self, name: str) -> Callable[..., Table] | VariableGroupBy:
        # Calling method on the groupby object
        if isinstance(getattr(self.groupby, name), types.MethodType):

            def func(*args, **kwargs):
                """Apply function and return variable with proper metadata."""
                df = getattr(self.groupby, name)(*args, **kwargs)
                if df.ndim == 1:
                    # output is series, e.g. `size` function
                    return df
                else:
                    tb = _create_table(df, self.metadata, self._fields)
                    if pl.enabled():
                        for col in tb.columns:
                            # parents are grouping columns and grouped one
                            index_parents = [tb.get_column_or_index(n) for n in tb.index.names]
                            tb[col].update_log(
                                operation=f"groupby_{name}",
                                parents=[tb[col]] + index_parents,
                            )
                    return tb

            self.__annotations__[name] = Callable[..., Table]
            return func
        else:
            self.__annotations__[name] = VariableGroupBy
            return VariableGroupBy(getattr(self.groupby, name), name, self._fields[name], self.metadata)

    @overload
    def __getitem__(self, key: str) -> VariableGroupBy: ...

    @overload
    def __getitem__(self, key: list) -> TableGroupBy: ...

    def __getitem__(self, key: str | list) -> VariableGroupBy | TableGroupBy:
        if isinstance(key, list):
            return TableGroupBy(self.groupby[key], self.metadata, self._fields)
        else:
            self.__annotations__[key] = VariableGroupBy
            return VariableGroupBy(self.groupby[key], key, self._fields[key], self.metadata)

    def __iter__(self) -> Iterator[tuple[Any, Table]]:
        for name, group in self.groupby:
            yield name, _create_table(group, self.metadata, self._fields)

    def agg(self, func: Any | None = None, *args, **kwargs) -> Table:
        df = self.groupby.agg(func, *args, **kwargs)
        tb = _create_table(df, self.metadata, self._fields)

        # kwargs rename fields
        for new_col, (col, _) in kwargs.items():
            tb._fields[new_col] = self._fields[col]

        if pl.enabled():
            for col in tb.columns:
                # parents are grouping columns and grouped one
                index_parents = [tb.get_column_or_index("b") for n in tb.index.names]
                tb[col].update_log(
                    operation=f"agg_{func}",
                    parents=[tb[col]] + index_parents,
                )

        return tb

    def apply(self, func: Callable[..., Any], *args, include_groups=True, **kwargs) -> Table | variables.Variable:
        mem = {}

        @wraps(func)
        def f(g):
            tb = func(g, *args, **kwargs)
            # remember one table to use its metadata
            if not mem:
                mem["table"] = tb
            return tb

        df = self.groupby.apply(f, *args, include_groups=include_groups)
        if not mem:
            return Table(df)
        elif type(mem["table"]) is pd.DataFrame:
            return _create_table(df, self.metadata, self._fields)
        elif type(mem["table"]) is pd.Series:
            if isinstance(df, Table):
                return _create_table(df, self.metadata, self._fields)
            else:
                return variables.Variable(df)
        elif isinstance(mem["table"], Table):
            return _create_table(df, mem["table"].metadata, mem["table"]._fields)
        elif isinstance(mem["table"], variables.Variable) and isinstance(df, variables.Variable):
            df.metadata = mem["table"].metadata
            return df
        elif isinstance(mem["table"], variables.Variable) and isinstance(df, Table):
            # func returns Variable
            out = _create_table(df, self.metadata, self._fields)
            if mem["table"].name and mem["table"].name in out.columns:
                out[mem["table"].name].metadata = mem["table"].metadata
            return out
        else:
            # func returns a scalar, output is a Table
            return _create_table(df, self.metadata, self._fields)

    def rolling(self, *args, **kwargs) -> "TableRollingGroupBy":
        rolling_groupby = self.groupby.rolling(*args, **kwargs)
        return TableRollingGroupBy(rolling_groupby, self.metadata, self._fields)


class VariableGroupBy:
    def __init__(
        self, groupby: pd.core.groupby.SeriesGroupBy, name: str, metadata: VariableMeta, table_metadata: TableMeta
    ):
        self.groupby = groupby
        self.metadata = metadata
        self.name = name
        self.table_metadata = table_metadata

    def __getattr__(self, funcname) -> Callable[..., Table]:
        if funcname == "groupings":
            return self.groupby.groupings

        def func(*args, **kwargs):
            """Apply function and return variable with proper metadata."""
            # out = getattr(self.groupby, funcname)(*args, **kwargs)
            ff = getattr(self.groupby, funcname)
            out = ff(*args, **kwargs)

            # this happens when we use e.g. agg([min, max]), propagate metadata from the original then
            if isinstance(out, Table):
                out._fields = defaultdict(VariableMeta, {k: self.metadata for k in out.columns})
                out.metadata = self.table_metadata.copy()
                return out
            elif isinstance(out, variables.Variable):
                out.metadata = self.metadata.copy()
                return out
            elif isinstance(out, pd.Series):
                return variables.Variable(out, name=self.name, metadata=self.metadata)
            else:
                raise NotImplementedError()

        return func  # type: ignore

    def rolling(self, *args, **kwargs) -> "VariableGroupBy":
        """Apply rolling window function and return a new VariableGroupBy with proper metadata."""
        rolling_groupby = self.groupby.rolling(*args, **kwargs)
        return VariableGroupBy(rolling_groupby, self.name, self.metadata, self.table_metadata)


class TableRolling:
    # fixes type hints
    __annotations__ = {}

    def __init__(self, rolling: pd.core.window.rolling.Rolling, metadata: TableMeta, fields: dict[str, Any]):
        self.rolling = rolling
        self.metadata = metadata
        self._fields = fields

    def __getattr__(self, name: str) -> Callable[..., Table]:
        # Calling method on the rolling object
        if isinstance(getattr(self.rolling, name), types.MethodType):

            def func(*args, **kwargs):
                """Apply function and return variable with proper metadata."""
                df = getattr(self.rolling, name)(*args, **kwargs)
                return _create_table(df, self.metadata, self._fields)

            self.__annotations__[name] = Callable[..., Table]
            return func
        else:
            raise NotImplementedError()


class TableRollingGroupBy:
    # fixes type hints
    __annotations__ = {}

    def __init__(
        self, rolling_groupby: pd.core.window.rolling.RollingGroupby, metadata: TableMeta, fields: dict[str, Any]
    ):
        self.rolling_groupby = rolling_groupby
        self.metadata = metadata
        self._fields = fields

    def __getattr__(self, name: str) -> Callable[..., Table]:
        # Calling method on the rolling object
        if isinstance(getattr(self.rolling_groupby, name), types.MethodType):

            def func(*args, **kwargs):
                """Apply function and return variable with proper metadata."""
                df = getattr(self.rolling_groupby, name)(*args, **kwargs)
                return _create_table(df, self.metadata, self._fields)

            self.__annotations__[name] = Callable[..., Table]
            return func
        else:
            raise NotImplementedError()


def align_categoricals(left: SeriesOrVariable, right: SeriesOrVariable) -> tuple[SeriesOrVariable, SeriesOrVariable]:
    """Align categorical columns if possible. If not, return originals. This is necessary for
    efficient merging."""
    if left.dtype.name == "category" and right.dtype.name == "category":
        common_categories = left.cat.categories.union(right.cat.categories)

        if isinstance(left, variables.Variable):
            left = left.set_categories(common_categories)
        else:
            left = left.cat.set_categories(common_categories)

        if isinstance(right, variables.Variable):
            right = right.set_categories(common_categories)
        else:
            right = right.cat.set_categories(common_categories)

        return left, right
    else:
        return left, right


def merge(
    left,
    right,
    how="inner",
    on=None,
    left_on=None,
    right_on=None,
    suffixes=("_x", "_y"),
    short_name: str | None = None,
    **kwargs,
) -> Table:
    if ("left_index" in kwargs) or ("right_index" in kwargs):
        # TODO: Arguments left_index/right_index are not implemented.
        raise NotImplementedError(
            "Arguments 'left_index' and 'right_index' currently not implemented in function 'merge'."
        )

    # If arguments "on", "left_on", or "right_on" are given as strings, convert them to lists.
    if isinstance(on, str):
        on = [on]
    if isinstance(left_on, str):
        left_on = [left_on]
    if isinstance(right_on, str):
        right_on = [right_on]

    # Align categorical columns to make them survive pd.merge.
    if left_on and right_on:
        lefts_rights = zip(left_on, right_on)
    elif on:
        lefts_rights = zip(on, on)
    else:
        lefts_rights = []

    # copy to avoid warnings
    left = left.copy(deep=False)
    right = right.copy(deep=False)
    for left_col, right_col in lefts_rights:
        left[left_col], right[right_col] = align_categoricals(left[left_col], right[right_col])

    # Create merged table.
    tb = Table(
        # There's a weird bug that removes metadata of the left table. I could not replicate it with unit test
        # It is necessary to copy metadata here to avoid mutating passed left.
        pd.merge(
            left=left.copy(deep=False),
            right=right.copy(deep=False),
            how=how,
            on=on,
            left_on=left_on,
            right_on=right_on,
            suffixes=suffixes,
            **kwargs,
        )
    )

    if (on is None) and (left_on is None):
        # By construction, either "on" is passed, or both "left_on" and "right_on".
        # Any other possibility will raise a MergeError, and hence doesn't need to be considered.
        # If none of them is not specified, assume we are joining on common columns.
        on = list(set(left.all_columns) & set(right.all_columns))
        left_on = []
        right_on = []
    elif left_on is not None:
        # By construction, right_on must also be given.
        on = []
    else:
        # Here, "on" is given, but not "left_on".
        left_on = []
        right_on = []

    # Find columns that existed in both left and right tables whose name will be modified with suffixes.
    overlapping_columns = ((set(left.all_columns) - set(left_on)) & (set(right.all_columns) - set(right_on))) - set(on)  # type: ignore

    # Find columns that existed in both left and right tables that will preserve their names (since they are columns to join on).
    common_columns = on or (set(left_on) & set(right_on))  # type: ignore

    columns_from_left = set(left.all_columns) - set(common_columns)
    columns_from_right = set(right.all_columns) - set(common_columns)

    for column in columns_from_left:
        if column in overlapping_columns:
            new_column = f"{column}{suffixes[0]}"
        else:
            new_column = column
        tb[new_column].metadata = variables.combine_variables_metadata([left[column]], operation="merge", name=column)

    for column in columns_from_right:
        if column in overlapping_columns:
            new_column = f"{column}{suffixes[1]}"
        else:
            new_column = column
        tb[new_column].metadata = variables.combine_variables_metadata([right[column]], operation="merge", name=column)

    for column in common_columns:
        tb[column].metadata = variables.combine_variables_metadata(
            [left[column], right[column]], operation="merge", name=column
        )

    # Update table metadata.
    tb.metadata = combine_tables_metadata(tables=[left, right], short_name=short_name)

    return tb


def concat(
    objs: list[Table],
    *,
    axis: int | str = 0,
    join: str = "outer",
    ignore_index: bool = False,
    short_name: str | None = None,
    **kwargs,
) -> Table:
    # TODO: Add more logic to this function to handle indexes and possibly other arguments.
    with warnings.catch_warnings():
        warnings.simplefilter(action="ignore", category=FutureWarning)
        table = Table(
            # use our concatenate that gracefully handles categoricals
            dataframes.concatenate(
                objs=objs,  # type: ignore
                axis=axis,  # type: ignore
                join=join,
                ignore_index=ignore_index,
                **kwargs,
            )
        )
        ################################################################################################################
        # In pandas 2.2.1, pd.concat() does not return a copy when one of the input dataframes is empty.
        # This causes the following unexpected behavior:
        # df_0 = pd.DataFrame({"a": ["original value"]})
        # df_1 = pd.concat([pd.DataFrame(), df_0], ignore_index=True)
        # df_0.loc[:, "a"] = "new value"
        # df_1["a"]  # This will return "new value" instead of "original value".
        # In pandas `1.4.0`, the behavior was as expected (returning "original value").
        # Note that this happens even if `copy=True` is passed to `pd.concat()`.
        if any([len(obj) == 0 for obj in objs]):
            if pd.__version__ != "2.2.1":
                # Check if patch is no longer needed.
                df_0 = pd.DataFrame({"a": ["original value"]})
                # use our concatenate that gracefully handles categoricals
                df_1 = dataframes.concatenate([pd.DataFrame(), df_0], ignore_index=True)
                df_0.loc[:, "a"] = "new value"
                if df_1["a"].item() != "new value":
                    log.warning("Remove patch in owid.catalog.tables.concat, which is no longer necessary.")
            # Ensure concat returns a copy.
            table = table.copy()
        ################################################################################################################

    if (axis == 1) or (axis == "columns"):
        # Original function pd.concat allows returning a dataframe with multiple columns with the same name.
        # But this should not be allowed (and metadata cannot be stored for different variables if they have the same name).
        repeated_columns = table.columns[table.columns.duplicated()].tolist()
        if len(repeated_columns) > 0:
            raise KeyError(f"Concatenated table contains repeated columns: {repeated_columns}")

        # Assign variable metadata from input tables.
        for table_i in objs:
            for column in table_i.columns:
                table[column].metadata = table_i[column].metadata

    else:
        if list(filter(None, table.index.names)):
            raise NotImplementedError("Concatenation of tables with index is not implemented.")

        # Add to each column either the metadata of the original variable (if the variable appeared only in one of the input
        # tables) or the combination of the metadata from different tables (if the variable appeared in various tables).
        for column in table.all_columns:
            variables_to_combine = [table_i[column] for table_i in objs if column in table_i.all_columns]
            table._fields[column] = variables.combine_variables_metadata(
                variables=variables_to_combine, operation="concat", name=column
            )

    # Update table metadata.
    table.metadata = combine_tables_metadata(tables=objs, short_name=short_name)

    return table


def melt(
    frame: Table,
    id_vars: tuple[str] | list[str] | str | None = None,
    value_vars: tuple[str] | list[str] | str | None = None,
    var_name: str = "variable",
    value_name: str = "value",
    short_name: str | None = None,
    *args,
    **kwargs,
) -> Table:
    # TODO: We may need to implement some mor logic here to handle multi-index dataframes.
    # Get the new melt table.
    table = Table(
        pd.melt(
            frame=frame,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name,
            *args,
            **kwargs,
        )
    )

    # Get the list of column names used as id variables.
    if id_vars is None:
        id_vars_list = []
    elif isinstance(id_vars, str):
        id_vars_list: list[str] = [id_vars]
    else:
        id_vars_list = id_vars  # type: ignore

    # Get the list of column names used as id and value variables.
    if value_vars is None:
        value_vars_list = [column for column in frame.columns if column not in id_vars_list]
    elif isinstance(value_vars, str):
        value_vars_list = [value_vars]
    else:
        value_vars_list = value_vars  # type: ignore

    # Combine metadata of value variables and assign the combination to the new "value" column.
    table[value_name].metadata = variables.combine_variables_metadata(
        variables=[frame[var] for var in value_vars_list], operation="melt", name=value_name
    )

    # Assign that combined metadata also to the new "variable" column.
    table[var_name].metadata = variables.combine_variables_metadata(
        variables=[frame[var] for var in value_vars_list], operation="melt", name=var_name
    )

    for variable in id_vars_list:
        # Combine metadata of id variables and assign the combination to the new "id" variable.
        table[variable].metadata = variables.combine_variables_metadata(
            variables=[frame[variable]], operation="melt", name=variable
        )

    # Update table metadata.
    table.metadata = combine_tables_metadata(tables=[frame], short_name=short_name)

    return table


def _flatten_multiindex_column_names(table: Table, join_column_levels_with: str) -> list[str]:
    new_columns = []
    for column in table.columns:
        if isinstance(column, tuple):
            levels = [str(level) for level in column if len(str(level)) > 0]
            new_column = join_column_levels_with.join(levels)
        else:
            new_column = column
        new_columns.append(new_column)

    return new_columns


def pivot(
    data: Table,
    *,
    index: str | list[str] | None = None,
    columns: str | list[str] | None = None,
    values: str | list[str] | None = None,
    join_column_levels_with: str | None = None,
    short_name: str | None = None,
    fill_dimensions: bool = True,
    **kwargs,
) -> Table:
    if index is not None:
        kwargs["index"] = index
    if columns is not None:
        kwargs["columns"] = columns
    if values is not None:
        kwargs["values"] = values

    # Get the new pivot table.
    table = Table(
        pd.pivot(
            data=data,
            **kwargs,
        )
    )

    # Update variable metadata in the new table.
    for column in table.columns:
        if isinstance(values, str):
            column_name = values
        else:
            column_name = column[0]
        variables_to_combine = [data[column_name]]
        # variables_to_combine = _extract_variables(data, columns) + _extract_variables(data, values)

        # For now, I assume the only metadata we want to propagate is the one of the upper level.
        # Alternatively, we could combine the metadata of the upper level variable with the metadata of the original
        # variable of all subsequent levels.
        column_metadata = variables.combine_variables_metadata(
            variables=variables_to_combine, operation="pivot", name=column
        )
        # Assign metadata of the original variable in the upper level to the new multiindex column.
        # NOTE: This allows accessing the metadata via, e.g. `table[("level_0", "level_1", "level_2")].metadata`,
        # but not via, e.g. `table["level_0"]["level_1"]["level_2"].metadata`.
        # There may be a way to allow for both.
        table[column].metadata = column_metadata

        # Optionally fill dimensions metadata
        if fill_dimensions:
            if isinstance(column, tuple) and isinstance(columns, list):
                table[column].m.dimensions = dict(zip(columns, column))

    # Transfer also the metadata of the index columns.
    # Note: This metadata will only be accessible if columns are reset and flattened to one level.
    for index_column in list(table.index.names):
        table._fields[index_column] = data._fields[index_column]

    if join_column_levels_with is not None:
        # Gather metadata of index columns.
        index_metadata = [table._fields[index_column] for index_column in table.index.names]
        # Gather metadata of each multiindex column.
        columns_metadata = [table[column].metadata for column in table.columns]
        # Reset index (which can create multi-index columns).
        table = table.reset_index()
        # Join column levels with a certain string, e.g. ("level_0", "level_1", "level_2") -> "level_0-level_1-level_2".
        table.columns = _flatten_multiindex_column_names(table, join_column_levels_with=join_column_levels_with)
        # Assign the gathered metadata for indexes and columns to the corresponding new columns.
        for i, column in enumerate(table.columns):
            table[column].metadata = (index_metadata + columns_metadata)[i]

    # Update table metadata.
    table.metadata = combine_tables_metadata(tables=[data], short_name=short_name)

    return table


def _add_table_and_variables_metadata_to_table(
    table: Table, metadata: TableMeta | None, origin: Origin | None
) -> Table:
    if metadata is not None:
        table.metadata = metadata.copy()
        for column in list(table.all_columns):
            if origin:
                table._fields[column].origins = [origin]
            else:
                table._fields[column].sources = metadata.dataset.sources  # type: ignore
            table._fields[column].licenses = metadata.dataset.licenses  # type: ignore
    table = update_processing_logs_when_loading_or_creating_table(table=table)

    return table


def read_csv(
    filepath_or_buffer: str | Path | IO[AnyStr],
    metadata: TableMeta | None = None,
    origin: Origin | None = None,
    underscore: bool = False,
    *args,
    **kwargs,
) -> Table:
    table = Table(pd.read_csv(filepath_or_buffer=filepath_or_buffer, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return cast(Table, table)


def read_fwf(
    filepath_or_buffer: FilePath | ReadCsvBuffer[bytes] | ReadCsvBuffer[str],
    metadata: TableMeta | None = None,
    origin: Origin | None = None,
    underscore: bool = False,
    *args,
    **kwargs,
) -> Table:
    table = Table(pd.read_fwf(filepath_or_buffer=filepath_or_buffer, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return cast(Table, table)


def read_feather(
    filepath: str | Path | IO[AnyStr],
    metadata: TableMeta | None = None,
    origin: Origin | None = None,
    underscore: bool = False,
    *args,
    **kwargs,
) -> Table:
    table = Table(pd.read_feather(filepath, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return cast(Table, table)


def read_excel(
    io: str | Path,
    *args,
    metadata: TableMeta | None = None,
    origin: Origin | None = None,
    underscore: bool = False,
    **kwargs,
) -> Table:
    assert not isinstance(kwargs.get("sheet_name"), list), "Argument 'sheet_name' must be a string or an integer."
    table = Table(pd.read_excel(io=io, *args, **kwargs), underscore=underscore)
    # Note: Maybe we should include the sheet name in parents.
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return cast(Table, table)


def read_from_records(
    data: Any,
    *args,
    metadata: TableMeta | None = None,
    origin: Origin | None = None,
    underscore: bool = False,
    **kwargs,
):
    table = Table(pd.DataFrame.from_records(data=data, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return table


def read_from_dict(
    data: dict[Any, Any],
    *args,
    metadata: TableMeta | None = None,
    origin: Origin | None = None,
    underscore: bool = False,
    **kwargs,
) -> Table:
    table = Table(pd.DataFrame.from_dict(data=data, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return table


def read_from_df(
    data: pd.DataFrame,
    metadata: TableMeta | None = None,
    origin: Origin | None = None,
    underscore: bool = False,
) -> Table:
    table = Table(data, underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return table


def read_json(
    path_or_buf: str | Path | IO[AnyStr],
    metadata: TableMeta | None = None,
    origin: Origin | None = None,
    underscore: bool = False,
    *args,
    **kwargs,
) -> Table:
    table = Table(pd.read_json(path_or_buf=path_or_buf, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return cast(Table, table)


def read_stata(
    filepath_or_buffer: str | Path | IO[AnyStr],
    metadata: TableMeta | None = None,
    origin: Origin | None = None,
    underscore: bool = False,
    *args,
    **kwargs,
) -> Table:
    table = Table(pd.read_stata(filepath_or_buffer=filepath_or_buffer, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return cast(Table, table)


def read_rda(
    filepath_or_buffer: str | Path | IO[AnyStr],
    table_name: str,
    metadata: TableMeta | None = None,
    origin: Origin | None = None,
    underscore: bool = False,
) -> Table:
    parsed = rdata.parser.parse_file(filepath_or_buffer)  # type: ignore
    converted = rdata.conversion.convert(parsed)

    if table_name not in converted:
        raise ValueError(f"Table {table_name} not found in RDA file.")
    table = Table(converted[table_name], underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return cast(Table, table)


def read_rda_multiple(
    filepath_or_buffer: str | Path | IO[AnyStr],
    table_names: list[str] | None = None,
    metadata: TableMeta | None = None,
    origin: Origin | None = None,
    underscore: bool = False,
) -> dict[str, Table]:
    # Read RData
    parsed = rdata.parser.parse_file(filepath_or_buffer)  # type: ignore
    converted = rdata.conversion.convert(parsed)

    # Init output dictionary
    tables = {}

    if table_names is not None:
        # If table names are given, read them
        for tname in table_names:
            # Check that name exists in RDA file
            if tname not in converted:
                raise ValueError(f"Table {tname} not found in RDA file.")
            # Load object
            table = converted[tname]
            # Check that object is a DataFrame (otherwise raise error!). NOTE: here we raise an error, bc user explicitly asked us to load this table.
            if isinstance(table, pd.DataFrame):
                raise ValueError(f"Table {tname} is not a DataFrame.")
            # Parse object to Table, and add metadata
            table = Table(converted[tname], underscore=underscore)
            table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
            table.metadata.short_name = tname
            # Safe table in main dictionary object
            tables[tname] = cast(Table, table)
    else:
        # Read them all (only those objects that are Dataframes)
        for fname, data in converted.items():
            if isinstance(data, pd.DataFrame):
                # Parse object to Table, and add metadata
                table = Table(data, underscore=underscore)
                table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
                table.metadata.short_name = fname
                # Safe table in main dictionary object
                tables[fname] = cast(Table, table)

    return tables


def read_rds(
    filepath_or_buffer: str | Path | IO[AnyStr],
    metadata: TableMeta | None = None,
    origin: Origin | None = None,
    underscore: bool = False,
) -> Table:
    parsed = rdata.parser.parse_file(filepath_or_buffer, extension="rds")  # type: ignore
    converted = rdata.conversion.convert(parsed)

    table = Table(converted, underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return cast(Table, table)


def read_parquet(
    filepath_or_buffer: str | Path | IO[AnyStr],
    metadata: TableMeta | None = None,
    origin: Origin | None = None,
    underscore: bool = False,
    *args,
    **kwargs,
) -> Table:
    table = Table(pd.read_parquet(path=filepath_or_buffer, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return cast(Table, table)


class ExcelFile(pd.ExcelFile):
    def __init__(self, *args, metadata: TableMeta | None = None, origin: Origin | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.metadata = metadata
        self.origin = origin

    def parse(
        self,
        sheet_name: str | int = 0,
        *args,
        metadata: TableMeta | None = None,
        origin: Origin | None = None,
        underscore: bool = False,
        **kwargs,
    ):
        metadata = metadata or self.metadata
        origin = origin or self.origin

        # Note: Maybe we should include the sheet name in parents.
        df = super().parse(sheet_name=sheet_name, *args, **kwargs)  # type: ignore
        table = Table(df, underscore=underscore, short_name=str(sheet_name))
        if metadata is not None:
            table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
        return table


def update_processing_logs_when_loading_or_creating_table(table: Table) -> Table:
    # Add entry to processing log, specifying that each variable was loaded from this table.
    try:
        # If the table comes from an ETL dataset, generate a URI for the table.
        table_uri = f"{table.metadata.dataset.uri}/{table.metadata.short_name}"  # type: ignore
        parents = [table_uri]
        operation = "load"
    except (AssertionError, AttributeError):
        # The table doesn't have an uri, which means it was probably created from scratch.
        parents = []
        operation = "create"

    # Add log entries to all columns
    for column in list(table.all_columns):
        pl = table._fields[column].processing_log

        # Clear processing log, we're not keeping log from previous channels. It can always be reconstructed
        # by concatenating processing log of indicators accross channels.
        pl.clear()
        pl.add_entry(
            variable=column,
            parents=parents,
            operation=operation,
        )

    return table


def update_processing_logs_when_saving_table(table: Table, path: str | Path) -> Table:
    # Infer the ETL uri from the path where the table will be saved.
    # Note: If the path does not fit the expected format, the result will be an arbitrary path, but it will not raise an
    # error, as long as path is a Path.
    path = Path(path)
    uri = "/".join(path.absolute().parts[-5:-1] + tuple([path.stem]))

    # Add a processing log entry to each column, including index columns.
    for column in list(table.all_columns):
        table._fields[column].processing_log.add_entry(
            target=uri,
            parents=[table._fields[column]],
            operation="save",
            variable=column,
        )

    return table


def copy_metadata(from_table: Table, to_table: Table, deep=False) -> Table:
    """Copy metadata from a different table to self."""
    tab = Table(pd.DataFrame.copy(to_table, deep=deep), metadata=from_table.metadata.copy())

    common_columns = set(to_table.all_columns) & set(from_table.all_columns)

    new_fields = defaultdict(VariableMeta)
    for k in common_columns:
        # copy if we have metadata in the other table
        if k in from_table._fields:
            new_fields[k] = from_table._fields[k].copy()
        # otherwise keep current metadata (if it exists)
        elif k in to_table._fields:
            new_fields[k] = to_table._fields[k]

    tab._fields = new_fields
    return tab


def get_unique_sources_from_tables(tables: Iterable[Table]) -> list[Source]:
    # Make a list of all sources of all variables in all tables.
    sources = []
    for table in tables:
        for column in list(table.all_columns):
            # Get unique array of tuples of source fields (respecting the order).
            sources += [source for source in table._fields[column].sources if source not in sources]
    return sources


def get_unique_licenses_from_tables(tables: Iterable[Table]) -> list[License]:
    # Make a list of all licenses of all variables in all tables.
    licenses = []
    for table in tables:
        for column in list(table.all_columns):
            # Get unique array of tuples of source fields (respecting the order).
            licenses += [license for license in table._fields[column].licenses if license not in licenses]
    return licenses


def _get_metadata_value_from_tables_if_all_identical(tables: Iterable[Table], field: str) -> Any | None:
    # Get unique values from list, ignoring Nones.
    unique_values = {getattr(table.metadata, field) for table in tables if getattr(table.metadata, field) is not None}
    if len(unique_values) == 1:
        combined_value = unique_values.pop()
    else:
        combined_value = None

    return combined_value


def combine_tables_title(tables: Iterable[Table]) -> str | None:
    return _get_metadata_value_from_tables_if_all_identical(tables=tables, field="title")


def combine_tables_description(tables: Iterable[Table]) -> str | None:
    return _get_metadata_value_from_tables_if_all_identical(tables=tables, field="description")


def combine_tables_datasetmeta(tables: Iterable[Table]) -> DatasetMeta | None:
    return _get_metadata_value_from_tables_if_all_identical(tables=tables, field="dataset")


def combine_tables_metadata(tables: list[Table], short_name: str | None = None) -> TableMeta:
    title = combine_tables_title(tables=tables)
    description = combine_tables_description(tables=tables)
    dataset = combine_tables_datasetmeta(tables=tables)
    if short_name is None:
        # If a short name is not specified, take it from the first table.
        short_name = tables[0].metadata.short_name
    metadata = TableMeta(title=title, description=description, short_name=short_name, dataset=dataset)

    return metadata


def combine_tables_update_period_days(tables: Iterable[Table]) -> int | None:
    # NOTE: This is a metadata field that is extracted from the dataset, not the table itself.

    # Gather all update_period_days from all tables (technically, from their dataset metadata).
    update_period_days_gathered = [
        getattr(table.metadata.dataset, "update_period_days")
        for table in tables
        if getattr(table.metadata, "dataset") and getattr(table.metadata.dataset, "update_period_days")
    ]
    if len(update_period_days_gathered) > 0:
        # Get minimum period of all tables.
        update_period_days_combined = min(update_period_days_gathered)
    else:
        # If no table had update_period_days defined, return None.
        update_period_days_combined = None

    return update_period_days_combined


def check_all_variables_have_metadata(tables: Iterable[Table], fields: list[str] | None = None) -> None:
    if fields is None:
        fields = ["origins"]

    for table in tables:
        table_name = table.metadata.short_name
        for column in table.columns:
            for field in fields:
                if not getattr(table[column].metadata, field):
                    warning_class = warnings.NoOriginsWarning if field == "origins" else warnings.MetadataWarning
                    warnings.warn(f"Table {table_name}, column {column} has no {field}.", warning_class)


def _resolve_collisions(
    orig_cols: pd.Index,
    new_cols: pd.Index,
    collision: Literal["raise", "rename", "ignore"],
) -> pd.Index:
    new_cols = new_cols.copy()
    vc = new_cols.value_counts()

    colliding_cols = list(vc[vc >= 2].index)
    for colliding_col in colliding_cols:
        ixs = np.where(new_cols == colliding_col)[0]
        if collision == "raise":
            raise NameError(
                f"Columns `{orig_cols[ixs[0]]}` and `{orig_cols[ixs[1]]}` are given the same name "
                f"`{colliding_cols[0]}` after underscoring`"
            )
        elif collision == "rename":
            # give each column numbered suffix
            for i, ix in enumerate(ixs):
                new_cols.values[ix] = f"{new_cols[ix]}_{i + 1}"
        elif collision == "ignore":
            pass
        else:
            raise NotImplementedError()
    return new_cols


def multi_merge(tables: list[Table], *args, **kwargs) -> Table:
    """Merge multiple tables.

    This is a helper function when merging more than two tables on common columns.

    Parameters
    ----------
    tables : list[Table]
        Tables to merge.

    Returns
    -------
    combined : Table
        Merged table.

    """
    combined = tables[0].copy()
    for table in tables[1:]:
        combined = combined.merge(table, *args, **kwargs)

    return combined


def _extract_variables(t: Table, cols: list[str] | str | None) -> list[variables.Variable]:
    if not cols:
        return []
    if isinstance(cols, str):
        cols = [cols]
    return [t[col] for col in cols]  # type: ignore


def read_df(
    df: pd.DataFrame,
    metadata: TableMeta | None = None,
    origin: Origin | None = None,
    underscore: bool = False,
) -> Table:
    """Create a Table (with metadata and an origin) from a DataFrame.
    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    metadata : TableMeta | None, optional
        Table metadata (with a title and description).
    origin : Origin | None, optional
        Origin of the table.
    underscore : bool, optional
        True to ensure all column names are snake case.
    Returns
    -------
    Table
        Original data as a Table with metadata and an origin.
    """
    table = Table(df, underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return cast(Table, table)


def keep_metadata(func: Callable[..., pd.DataFrame | pd.Series]) -> Callable[..., Table | variables.Variable]:
    """Decorator that turns a function that works on DataFrame or Series into a function that works
    on Table or Variable and preserves metadata.  If the decorated function renames columns, their
    metadata won't be copied.

    Usage:

    import owid.catalog.processing as pr

    @pr.keep_metadata
    def my_df_func(df: pd.DataFrame) -> pd.DataFrame:
        return df + 1

    tb = my_df_func(tb)


    @pr.keep_metadata
    def my_series_func(s: pd.Series) -> pd.Series:
        return s + 1

    tb.a = my_series_func(tb.a)
    """

    def wrapper(*args: Any, **kwargs: Any) -> Table | variables.Variable:
        tb = args[0]
        df = func(*args, **kwargs)
        if isinstance(df, pd.Series):
            return variables.Variable(df, name=tb.name, metadata=tb.metadata)
        elif isinstance(df, pd.DataFrame):
            return Table(df).copy_metadata(tb)
        else:
            raise ValueError(f"Unexpected return type: {type(df)}")

    return wrapper


to_datetime = keep_metadata(pd.to_datetime)
to_numeric = keep_metadata(pd.to_numeric)


def update_variable_dimensions(variable, dimensions_data: dict[str, Any]) -> None:
    """
    Update a variable's dimensions metadata.

    Args:
        variable: The variable to update with dimension information
        dimensions_data: Dictionary containing dimension information
    """
    if dimensions_data:
        variable.m.original_short_name = dimensions_data.get("originalShortName")
        variable.m.original_title = dimensions_data.get("originalName")
        variable.m.dimensions = {f["name"]: f["value"] for f in dimensions_data.get("filters", [])}

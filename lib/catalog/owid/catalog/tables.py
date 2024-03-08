#
#  tables.py
#

import json
import types
from collections import defaultdict
from os.path import dirname, join, splitext
from pathlib import Path
from typing import (
    IO,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
    Union,
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
from pandas.util._decorators import rewrite_axis_style_signature

from owid.repack import repack_frame

from . import processing_log as pl
from . import variables
from .meta import (
    SOURCE_EXISTS_OPTIONS,
    License,
    Origin,
    Source,
    TableMeta,
    VariableMeta,
)
from .utils import underscore

log = structlog.get_logger()

SCHEMA = json.load(open(join(dirname(__file__), "schemas", "table.json")))
METADATA_FIELDS = list(SCHEMA["properties"])

# New type required for pandas reading functions.
AnyStr = TypeVar("AnyStr", str, bytes)


class Table(pd.DataFrame):
    # metdata about the entire table
    metadata: TableMeta

    # metadata about individual columns
    # NOTE: the name _fields is also on the Variable class, pandas will propagate this to
    #       any slices, which is how they get access to their metadata
    _fields: Dict[str, VariableMeta]

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
        metadata: Optional[TableMeta] = None,
        short_name: Optional[str] = None,
        underscore=False,
        camel_to_snake=False,
        like: Optional["Table"] = None,
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
    def primary_key(self) -> List[str]:
        return [n for n in self.index.names if n]

    def to(self, path: Union[str, Path], repack: bool = True) -> None:
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
    def read(cls, path: Union[str, Path]) -> "Table":
        if isinstance(path, Path):
            path = path.as_posix()

        if path.endswith(".csv"):
            table = cls.read_csv(path)

        elif path.endswith(".feather"):
            table = cls.read_feather(path)

        elif path.endswith(".parquet"):
            table = cls.read_parquet(path)
        else:
            raise ValueError(f"could not detect a suitable format to read from: {path}")

        # Add processing log to the metadata of each variable in the table.
        table = update_processing_logs_when_loading_or_creating_table(table=table)

        if cls.DEBUG:
            table.check_metadata()

        return table

    # Mypy complaints about this not matching the defintiion of NDFrame.to_csv but I don't understand why
    def to_csv(self, path: Any, **kwargs: Any) -> None:  # type: ignore
        """
        Save this table as a csv file plus accompanying JSON metadata file.
        If the table is stored at "mytable.csv", the metadata will be at
        "mytable.meta.json".
        """
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
            df = repack_frame(df)

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
            json.dump(metadata, ostream, indent=2, default=str)

    @classmethod
    def read_csv(cls, path: Union[str, Path]) -> "Table":
        """
        Read the table from csv plus accompanying JSON sidecar.
        """
        if isinstance(path, Path):
            path = path.as_posix()

        if not path.endswith(".csv"):
            raise ValueError(f'filename must end in ".csv": {path}')

        # load the data
        df = Table(pd.read_csv(path, index_col=False, na_values=[""], keep_default_na=False))

        # load the metadata
        metadata = cls._read_metadata(path)

        primary_key = metadata.pop("primary_key") if "primary_key" in metadata else []
        fields = metadata.pop("fields") if "fields" in metadata else {}

        df.metadata = TableMeta(**metadata)
        df._fields = defaultdict(VariableMeta, {k: VariableMeta.from_dict(v) for k, v in fields.items()})

        if primary_key:
            df.set_index(primary_key, inplace=True)

        return df

    def update_metadata(self, **kwargs) -> "Table":
        """Set Table metadata."""
        for k, v in kwargs.items():
            assert hasattr(self.metadata, k), f"unknown metadata field {k} in TableMeta"
            setattr(self.metadata, k, v)
        return self

    @classmethod
    def _add_metadata(cls, df: pd.DataFrame, path: str) -> None:
        """Read metadata from JSON sidecar and add it to the dataframe."""
        metadata = cls._read_metadata(path)

        primary_key = metadata.get("primary_key", [])
        fields = metadata.pop("fields") if "fields" in metadata else {}

        df.metadata = TableMeta.from_dict(metadata)
        df._set_fields_from_dict(fields)

        # NOTE: setting index is really slow for large datasets
        if primary_key:
            df.set_index(primary_key, inplace=True)

    @classmethod
    def read_feather(cls, path: Union[str, Path]) -> "Table":
        """
        Read the table from feather plus accompanying JSON sidecar.

        The path may be a local file path or a URL.
        """
        if isinstance(path, Path):
            path = path.as_posix()

        if not path.endswith(".feather"):
            raise ValueError(f'filename must end in ".feather": {path}')

        # load the data and add metadata
        df = Table(pd.read_feather(path))
        cls._add_metadata(df, path)
        return df

    @classmethod
    def read_parquet(cls, path: Union[str, Path]) -> "Table":
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
        cls._add_metadata(df, path)
        return df

    def _get_fields_as_dict(self) -> Dict[str, Any]:
        return {col: self._fields[col].to_dict() for col in self.all_columns}

    def _set_fields_from_dict(self, fields: Dict[str, Any]) -> None:
        self._fields = defaultdict(VariableMeta, {k: VariableMeta.from_dict(v) for k, v in fields.items()})

    @staticmethod
    def _read_metadata(data_path: str) -> Dict[str, Any]:
        import requests

        metadata_path = splitext(data_path)[0] + ".meta.json"

        if metadata_path.startswith("http"):
            return cast(Dict[str, Any], requests.get(metadata_path).json())

        with open(metadata_path, "r") as istream:
            return cast(Dict[str, Any], json.load(istream))

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
            else:
                self._fields[key] = VariableMeta()

        if self.DEBUG:
            self.check_metadata()

    def equals_table(self, table: "Table") -> bool:
        return (
            isinstance(table, Table)
            and self.metadata == table.metadata
            # By simply doing self.to_dict() == table.to_dict() may return False if the dictionaries are identical but
            # contain nans.
            and self.fillna(123456).to_dict() == table.fillna(123456).to_dict()
            and self._fields == table._fields
        )

    @rewrite_axis_style_signature(
        "mapper",
        [("copy", True), ("inplace", False), ("level", None), ("errors", "ignore")],
    )
    def rename(self, *args: Any, **kwargs: Any) -> Optional["Table"]:
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
    def all_columns(self) -> List[str]:
        "Return names of all columns in the dataset, including the index."
        combined: List[str] = filter(None, list(self.index.names) + list(self.columns))  # type: ignore
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
        path: Union[Path, str],
        table_name: str,
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
            if_origins_exist=if_origins_exist,
        )

    def prune_metadata(self) -> "Table":
        """Prune metadata for columns that are not in the table. This can happen after slicing
        the table by columns."""
        self._fields = defaultdict(VariableMeta, {col: self._fields[col] for col in self.all_columns})
        return self

    def copy(self, deep: bool = True) -> "Table":
        """Copy table together with all its metadata."""
        tab = super().copy(deep=deep)
        return tab.copy_metadata(self)

    def copy_metadata(self, from_table: "Table", deep: bool = False) -> "Table":
        """Copy metadata from a different table to self."""
        return copy_metadata(to_table=self, from_table=from_table, deep=deep)

    @overload
    def set_index(
        self,
        keys: Union[str, List[str]],
        *,
        inplace: Literal[True],
        **kwargs: Any,
    ) -> None:
        ...

    @overload
    def set_index(self, keys: Union[str, List[str]], *, inplace: Literal[False], **kwargs: Any) -> "Table":
        ...

    @overload
    def set_index(self, keys: Union[str, List[str]], **kwargs: Any) -> "Table":
        ...

    def set_index(
        self,
        keys: Union[str, List[str]],
        **kwargs: Any,
    ) -> Optional["Table"]:
        if isinstance(keys, str):
            keys = [keys]

        if kwargs.get("inplace"):
            super().set_index(keys, **kwargs)
            self.metadata.primary_key = keys
            return None
        else:
            t = super().set_index(keys, **kwargs)
            t.metadata.primary_key = keys
            return cast(Table, t)

    @overload
    def reset_index(self, *, inplace: Literal[True]) -> None:
        ...

    @overload
    def reset_index(self, *, inplace: Literal[False]) -> "Table":
        ...

    @overload
    def reset_index(self) -> "Table":
        ...

    def reset_index(self, *args, **kwargs) -> Optional["Table"]:  # type: ignore
        """Fix type signature of reset_index."""
        t = super().reset_index(*args, **kwargs)
        if kwargs.get("inplace"):
            return None
        else:
            # preserve metadata in _fields, calling reset_index() on a table drops it
            t._fields = self._fields
            return t  # type: ignore

    def join(self, other: Union[pd.DataFrame, "Table"], *args, **kwargs) -> "Table":
        """Fix type signature of join."""
        t = super().join(other, *args, **kwargs)

        t = t.copy_metadata(self)

        # copy variables metadata from other table
        if isinstance(other, Table):
            for k, v in other._fields.items():
                t._fields[k] = v.copy()
        return t  # type: ignore

    def _repr_html_(self):
        html = super()._repr_html_()
        if self.DEBUG:
            self.check_metadata()
        return f"""
             <h2 style="margin-bottom: 0em"><pre>{self.metadata.short_name}</pre></h2>
             <p style="font-variant: small-caps; font-size: 1.5em; font-family: sans-serif; color: grey; margin-top: -0.2em; margin-bottom: 0.2em">table</p>
             {html}
        """

    def merge(self, right, *args, **kwargs) -> "Table":
        return merge(left=self, right=right, *args, **kwargs)

    def melt(
        self,
        id_vars: Optional[Union[Tuple[str], List[str], str]] = None,
        value_vars: Optional[Union[Tuple[str], List[str], str]] = None,
        var_name: str = "variable",
        value_name: str = "value",
        short_name: Optional[str] = None,
        *args,
        **kwargs,
    ) -> "Table":
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
        index: Optional[Union[str, List[str]]] = None,
        columns: Optional[Union[str, List[str]]] = None,
        values: Optional[Union[str, List[str]]] = None,
        join_column_levels_with: Optional[str] = None,
        short_name: Optional[str] = None,
        **kwargs,
    ) -> "Table":
        return pivot(
            data=self,
            index=index,
            columns=columns,
            values=values,
            join_column_levels_with=join_column_levels_with,
            short_name=short_name,
            **kwargs,
        )

    def underscore(
        self,
        collision: Literal["raise", "rename", "ignore"] = "raise",
        inplace: bool = False,
        camel_to_snake: bool = False,
    ) -> "Table":
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
        new_cols = pd.Index([underscore(c, camel_to_snake=camel_to_snake) for c in t.columns])
        new_cols = _resolve_collisions(orig_cols, new_cols, collision)

        columns_map = {c_old: c_new for c_old, c_new in zip(orig_cols, new_cols)}
        if inplace:
            t.rename(columns=columns_map, inplace=True)
        else:
            t = t.rename(columns=columns_map)

        t.index.names = [underscore(e, camel_to_snake=camel_to_snake) for e in t.index.names]
        t.metadata.primary_key = t.primary_key
        t.metadata.short_name = underscore(t.metadata.short_name, camel_to_snake=camel_to_snake)

        # put original names as titles into metadata by default
        for c_old, c_new in columns_map.items():
            # if underscoring didn't change anything, don't add title
            if t[c_new].metadata.title is None and c_old != c_new:
                t[c_new].metadata.title = c_old

        return t

    def dropna(self, *args, **kwargs) -> Optional["Table"]:
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

        return cast("Table", tb)

    def drop(self, *args, **kwargs) -> "Table":
        return cast(Table, super().drop(*args, **kwargs))

    def update_log(
        self,
        operation: str,
        parents: Optional[List[Any]] = None,
        variable_names: Optional[List[str]] = None,
        comment: Optional[str] = None,
    ) -> "Table":
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
        variable_names: Optional[List[str]] = None,
        comment: Optional[str] = None,
        operation: Optional[str] = None,
    ) -> "Table":
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

    def sort_values(self, by: Union[str, List[str]], *args, **kwargs) -> "Table":
        tb = super().sort_values(by=by, *args, **kwargs).copy()
        for column in list(tb.all_columns):
            if isinstance(by, str):
                parents = [by, column]
            else:
                parents = by + [column]

            parent_variables = [tb.get_column_or_index(parent) for parent in parents]

            tb._fields[column].processing_log.add_entry(variable=column, parents=parent_variables, operation="sort")

        return cast("Table", tb)

    def sum(self, *args, **kwargs) -> variables.Variable:
        variable_name = variables.UNNAMED_VARIABLE
        variable = variables.Variable(super().sum(*args, **kwargs), name=variable_name)
        variable.metadata = variables.combine_variables_metadata(
            variables=[self[column] for column in self.columns], operation="+", name=variable_name
        )

        return variable

    def prod(self, *args, **kwargs) -> variables.Variable:
        variable_name = variables.UNNAMED_VARIABLE
        variable = variables.Variable(super().prod(*args, **kwargs), name=variable_name)
        variable.metadata = variables.combine_variables_metadata(
            variables=[self[column] for column in self.columns], operation="*", name=variable_name
        )

        return variable

    def assign(self, *args, **kwargs) -> "Table":
        return super().assign(*args, **kwargs)  # type: ignore

    @staticmethod
    def _update_log(tb: "Table", other: Union[Scalar, Series, variables.Variable, "Table"], operation: str) -> None:
        # The following would have a parents only the scalar, not the scalar and the corresponding variable.
        # tb = update_log(table=tb, operation="+", parents=[other], variable_names=tb.columns)
        # Instead, update the processing log of each variable in the table.
        for column in tb.columns:
            if isinstance(other, pd.DataFrame):
                parents = [tb[column], other[column]]
            else:
                parents = [tb[column], other]
            tb[column].update_log(parents=parents, operation=operation)

    def __add__(self, other: Union[Scalar, Series, variables.Variable, "Table"]) -> "Table":
        tb = cast(Table, Table(super().__add__(other=other)).copy_metadata(self))
        self._update_log(tb, other, "+")
        return tb

    def __iadd__(self, other: Union[Scalar, Series, variables.Variable, "Table"]) -> "Table":
        return self.__add__(other)

    def __sub__(self, other: Union[Scalar, Series, variables.Variable, "Table"]) -> "Table":
        tb = cast(Table, Table(super().__sub__(other=other)).copy_metadata(self))
        self._update_log(tb, other, "-")
        return tb

    def __isub__(self, other: Union[Scalar, Series, variables.Variable, "Table"]) -> "Table":
        return self.__sub__(other)

    def __mul__(self, other: Union[Scalar, Series, variables.Variable, "Table"]) -> "Table":
        tb = cast(Table, Table(super().__mul__(other=other)).copy_metadata(self))
        self._update_log(tb, other, "*")
        return tb

    def __imul__(self, other: Union[Scalar, Series, variables.Variable, "Table"]) -> "Table":
        return self.__mul__(other)

    def __truediv__(self, other: Union[Scalar, Series, variables.Variable, "Table"]) -> "Table":
        tb = cast(Table, Table(super().__truediv__(other=other)).copy_metadata(self))
        self._update_log(tb, other, "/")
        return tb

    def __itruediv__(self, other: Union[Scalar, Series, variables.Variable, "Table"]) -> "Table":
        return self.__truediv__(other)

    def __floordiv__(self, other: Union[Scalar, Series, variables.Variable, "Table"]) -> "Table":
        tb = cast(Table, Table(super().__floordiv__(other=other)).copy_metadata(self))
        self._update_log(tb, other, "//")
        return tb

    def __ifloordiv__(self, other: Union[Scalar, Series, variables.Variable, "Table"]) -> "Table":
        return self.__floordiv__(other)

    def __mod__(self, other: Union[Scalar, Series, variables.Variable, "Table"]) -> "Table":
        tb = cast(Table, Table(super().__mod__(other=other)).copy_metadata(self))
        self._update_log(tb, other, "%")
        return tb

    def __imod__(self, other: Union[Scalar, Series, variables.Variable, "Table"]) -> "Table":
        return self.__mod__(other)

    def __pow__(self, other: Union[Scalar, Series, variables.Variable, "Table"]) -> "Table":
        tb = cast(Table, Table(super().__pow__(other=other)).copy_metadata(self))
        self._update_log(tb, other, "**")
        return tb

    def __ipow__(self, other: Union[Scalar, Series, variables.Variable, "Table"]) -> "Table":
        return self.__pow__(other)

    def sort_index(self, *args, **kwargs) -> "Table":
        return super().sort_index(*args, **kwargs)  # type: ignore

    def groupby(self, *args, observed=False, **kwargs) -> "TableGroupBy":
        """Groupby that preserves metadata."""
        if observed is False and args:
            by_list = [args[0]] if isinstance(args[0], str) else args[0]
            for by in by_list:
                if isinstance(by, str):
                    try:
                        by_type = self.dtypes[by] if by in self.dtypes else self.index.dtypes[by]  # type: ignore
                    except AttributeError:
                        by_type = by
                elif isinstance(by, pd.Series):
                    by_type = by.dtype
                else:
                    by_type = "unknown"
                if isinstance(by_type, str) and by_type == "category":
                    log.warning(
                        f"You're grouping by categorical variable `{by}` without using observed=True. This may lead to unexpected behaviour."
                    )

        return TableGroupBy(
            pd.DataFrame.groupby(self.copy(deep=False), *args, observed=observed, **kwargs), self.metadata, self._fields
        )

    def check_metadata(self, ignore_columns: Optional[List[str]] = None) -> None:
        """Check that all variables in the table have origins."""
        if ignore_columns is None:
            if self.primary_key:
                ignore_columns = self.primary_key
            else:
                ignore_columns = ["year", "country"]

        for column in [column for column in self.columns if column not in ignore_columns]:
            if not self[column].metadata.origins:
                log.warning(f"Variable {column} has no origins.")

    def rename_index_names(self, renames: Dict[str, str]) -> "Table":
        """Rename index."""
        column_idx = list(self.index.names)
        column_idx_new = [renames.get(col, col) for col in column_idx]
        tb = self.reset_index().rename(columns=renames)
        tb = tb.set_index(column_idx_new)
        return tb

    def fillna(self, value, **kwargs) -> "Table":
        """Usual fillna, but, if the object given to fill values with is a table, transfer its metadata to the filled
        table."""
        tb = super().fillna(value, **kwargs)

        if type(value) == type(self):
            for column in tb.columns:
                if column in value.columns:
                    tb._fields[column] = variables.combine_variables_metadata(
                        variables=[tb[column], value[column]], operation="fillna", name=column
                    )

        return tb


def _create_table(df: pd.DataFrame, metadata: TableMeta, fields: Dict[str, VariableMeta]) -> Table:
    """Create a table with metadata."""
    tb = Table(df, metadata=metadata.copy())
    tb._fields = defaultdict(VariableMeta, fields)
    return tb


class TableGroupBy:
    # fixes type hints
    __annotations__ = {}

    def __init__(self, groupby: pd.core.groupby.DataFrameGroupBy, metadata: TableMeta, fields: Dict[str, Any]):
        self.groupby = groupby
        self.metadata = metadata
        self._fields = fields

    @overload
    def __getattr__(self, name: Literal["count", "size", "sum", "mean", "median"]) -> Callable[[], "Table"]:
        ...

    @overload
    def __getattr__(self, name: str) -> "VariableGroupBy":
        ...

    def __getattr__(self, name: str) -> Union[Callable[..., "Table"], "VariableGroupBy"]:
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

            self.__annotations__[name] = Callable[..., "Table"]
            return func
        else:
            self.__annotations__[name] = VariableGroupBy
            return VariableGroupBy(getattr(self.groupby, name), name, self._fields[name])

    @overload
    def __getitem__(self, key: str) -> "VariableGroupBy":
        ...

    @overload
    def __getitem__(self, key: list) -> "TableGroupBy":
        ...

    def __getitem__(self, key: Union[str, list]) -> Union["VariableGroupBy", "TableGroupBy"]:
        if isinstance(key, list):
            return TableGroupBy(self.groupby[key], self.metadata, self._fields)
        else:
            self.__annotations__[key] = VariableGroupBy
            return VariableGroupBy(self.groupby[key], key, self._fields[key])

    def __iter__(self) -> Iterator[Tuple[Any, "Table"]]:
        for name, group in self.groupby:
            yield name, _create_table(group, self.metadata, self._fields)

    def agg(self, func: Optional[Any] = None, *args, **kwargs) -> "Table":
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


class VariableGroupBy:
    def __init__(self, groupby: pd.core.groupby.SeriesGroupBy, name: str, metadata: VariableMeta):
        self.groupby = groupby
        self.metadata = metadata
        self.name = name

    def __getattr__(self, funcname) -> Callable[..., "Table"]:
        def func(*args, **kwargs):
            """Apply function and return variable with proper metadata."""
            # out = getattr(self.groupby, funcname)(*args, **kwargs)
            ff = getattr(self.groupby, funcname)
            out = ff(*args, **kwargs)

            # this happens when we use e.g. agg([min, max]), propagate metadata from the original then
            if isinstance(out, Table):
                out._fields = defaultdict(VariableMeta, {k: self.metadata for k in out.columns})
                return out
            elif isinstance(out, variables.Variable):
                out.metadata = self.metadata.copy()
                return out
            elif isinstance(out, pd.Series):
                return variables.Variable(out, name=self.name, metadata=self.metadata)
            else:
                raise NotImplementedError()

        return func  # type: ignore


def merge(
    left,
    right,
    how="inner",
    on=None,
    left_on=None,
    right_on=None,
    suffixes=("_x", "_y"),
    short_name: Optional[str] = None,
    **kwargs,
) -> Table:
    if ("left_index" in kwargs) or ("right_index" in kwargs):
        # TODO: Arguments left_index/right_index are not implemented.
        raise NotImplementedError(
            "Arguments 'left_index' and 'right_index' currently not implemented in function 'merge'."
        )
    # Create merged table.
    tb = Table(
        pd.merge(
            left=left, right=right, how=how, on=on, left_on=left_on, right_on=right_on, suffixes=suffixes, **kwargs
        )
    )

    # If arguments "on", "left_on", or "right_on" are given as strings, convert them to lists.
    if isinstance(on, str):
        on = [on]
    if isinstance(left_on, str):
        left_on = [left_on]
    if isinstance(right_on, str):
        right_on = [right_on]

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
    objs: List[Table],
    *,
    axis: Union[int, str] = 0,
    join: str = "outer",
    ignore_index: bool = False,
    short_name: Optional[str] = None,
    **kwargs,
) -> Table:
    # TODO: Add more logic to this function to handle indexes and possibly other arguments.
    table = Table(pd.concat(objs=objs, axis=axis, join=join, ignore_index=ignore_index, **kwargs))  # type: ignore

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
    id_vars: Optional[Union[Tuple[str], List[str], str]] = None,
    value_vars: Optional[Union[Tuple[str], List[str], str]] = None,
    var_name: str = "variable",
    value_name: str = "value",
    short_name: Optional[str] = None,
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
        id_vars_list: List[str] = [id_vars]
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


def _flatten_multiindex_column_names(table: Table, join_column_levels_with: str) -> List[str]:
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
    index: Optional[Union[str, List[str]]] = None,
    columns: Optional[Union[str, List[str]]] = None,
    values: Optional[Union[str, List[str]]] = None,
    join_column_levels_with: Optional[str] = None,
    short_name: Optional[str] = None,
    **kwargs,
) -> Table:
    # Get the new pivot table.
    table = Table(
        pd.pivot(
            data=data,
            index=index,
            columns=columns,
            values=values,
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
    table: Table, metadata: Optional[TableMeta], origin: Optional[Origin]
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
    filepath_or_buffer: Union[str, Path, IO[AnyStr]],
    metadata: Optional[TableMeta] = None,
    origin: Optional[Origin] = None,
    underscore: bool = False,
    *args,
    **kwargs,
) -> Table:
    table = Table(pd.read_csv(filepath_or_buffer=filepath_or_buffer, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return cast(Table, table)


def read_fwf(
    filepath_or_buffer: Union[FilePath, ReadCsvBuffer[bytes], ReadCsvBuffer[str]],
    metadata: Optional[TableMeta] = None,
    origin: Optional[Origin] = None,
    underscore: bool = False,
    *args,
    **kwargs,
) -> Table:
    table = Table(pd.read_fwf(filepath_or_buffer=filepath_or_buffer, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return cast(Table, table)


def read_feather(
    filepath: Union[str, Path, IO[AnyStr]],
    metadata: Optional[TableMeta] = None,
    origin: Optional[Origin] = None,
    underscore: bool = False,
    *args,
    **kwargs,
) -> Table:
    table = Table(pd.read_feather(filepath, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return cast(Table, table)


def read_excel(
    io: Union[str, Path],
    *args,
    metadata: Optional[TableMeta] = None,
    origin: Optional[Origin] = None,
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
    metadata: Optional[TableMeta] = None,
    origin: Optional[Origin] = None,
    underscore: bool = False,
    **kwargs,
):
    table = Table(pd.DataFrame.from_records(data=data, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return table


def read_from_dict(
    data: Dict[Any, Any],
    *args,
    metadata: Optional[TableMeta] = None,
    origin: Optional[Origin] = None,
    underscore: bool = False,
    **kwargs,
) -> Table:
    table = Table(pd.DataFrame.from_dict(data=data, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return table


def read_json(
    path_or_buf: Union[str, Path, IO[AnyStr]],
    metadata: Optional[TableMeta] = None,
    origin: Optional[Origin] = None,
    underscore: bool = False,
    *args,
    **kwargs,
) -> Table:
    table = Table(pd.read_json(path_or_buf=path_or_buf, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return cast(Table, table)


def read_stata(
    filepath_or_buffer: Union[str, Path, IO[AnyStr]],
    metadata: Optional[TableMeta] = None,
    origin: Optional[Origin] = None,
    underscore: bool = False,
    *args,
    **kwargs,
) -> Table:
    table = Table(pd.read_stata(filepath_or_buffer=filepath_or_buffer, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return cast(Table, table)


def read_rda(
    filepath_or_buffer: Union[str, Path, IO[AnyStr]],
    table_name: str,
    metadata: Optional[TableMeta] = None,
    origin: Optional[Origin] = None,
    underscore: bool = False,
) -> Table:
    parsed = rdata.parser.parse_file(filepath_or_buffer)  # type: ignore
    converted = rdata.conversion.convert(parsed)

    if table_name not in converted:
        raise ValueError(f"Table {table_name} not found in RDA file.")
    table = Table(converted[table_name], underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return cast(Table, table)


def read_rds(
    filepath_or_buffer: Union[str, Path, IO[AnyStr]],
    metadata: Optional[TableMeta] = None,
    origin: Optional[Origin] = None,
    underscore: bool = False,
) -> Table:
    parsed = rdata.parser.parse_file(filepath_or_buffer, extension="rds")  # type: ignore
    converted = rdata.conversion.convert(parsed)

    table = Table(converted, underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata, origin=origin)
    return cast(Table, table)


class ExcelFile(pd.ExcelFile):
    def __init__(self, *args, metadata: Optional[TableMeta] = None, origin: Optional[Origin] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.metadata = metadata
        self.origin = origin

    def parse(
        self,
        sheet_name: Union[str, int] = 0,
        *args,
        metadata: Optional[TableMeta] = None,
        origin: Optional[Origin] = None,
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


def update_processing_logs_when_saving_table(table: Table, path: Union[str, Path]) -> Table:
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


def get_unique_sources_from_tables(tables: List[Table]) -> List[Source]:
    # Make a list of all sources of all variables in all tables.
    sources = sum([table._fields[column].sources for table in tables for column in list(table.all_columns)], [])

    # Get unique array of tuples of source fields (respecting the order).
    return pd.unique(sources).tolist()


def get_unique_licenses_from_tables(tables: List[Table]) -> List[License]:
    # Make a list of all licenses of all variables in all tables.
    licenses = sum([table._fields[column].licenses for table in tables for column in list(table.all_columns)], [])

    # Get unique array of tuples of source fields (respecting the order).
    return pd.unique(licenses).tolist()


def _get_metadata_value_from_tables_if_all_identical(tables: List[Table], field: str) -> Optional[Any]:
    # Get unique values from list, ignoring Nones.
    unique_values = set(
        [getattr(table.metadata, field) for table in tables if getattr(table.metadata, field) is not None]
    )
    if len(unique_values) == 1:
        combined_value = unique_values.pop()
    else:
        combined_value = None

    return combined_value


def combine_tables_title(tables: List[Table]) -> Optional[str]:
    return _get_metadata_value_from_tables_if_all_identical(tables=tables, field="title")


def combine_tables_description(tables: List[Table]) -> Optional[str]:
    return _get_metadata_value_from_tables_if_all_identical(tables=tables, field="description")


def combine_tables_metadata(tables: List[Table], short_name: Optional[str] = None) -> TableMeta:
    title = combine_tables_title(tables=tables)
    description = combine_tables_description(tables=tables)
    if short_name is None:
        # If a short name is not specified, take it from the first table.
        short_name = tables[0].metadata.short_name
    metadata = TableMeta(title=title, description=description, short_name=short_name)

    return metadata


def combine_tables_update_period_days(tables: List[Table]) -> Optional[int]:
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


def check_all_variables_have_metadata(tables: List[Table], fields: Optional[List[str]] = None) -> None:
    if fields is None:
        fields = ["origins"]

    for table in tables:
        table_name = table.metadata.short_name
        for column in table.columns:
            for field in fields:
                if not getattr(table[column].metadata, field):
                    log.warning(f"Table {table_name}, column {column} has no {field}.")


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


def _extract_variables(t: Table, cols: Optional[Union[List[str], str]]) -> List[variables.Variable]:
    if not cols:
        return []
    if isinstance(cols, str):
        cols = [cols]
    return [t[col] for col in cols]  # type: ignore

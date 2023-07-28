#
#  tables.py
#

import copy
import dataclasses
import json
from collections import defaultdict
from os.path import dirname, join, splitext
from pathlib import Path
from typing import (
    IO,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
)

import pandas as pd
import pyarrow
import pyarrow.parquet as pq
import structlog
from owid.repack import repack_frame
from pandas.util._decorators import rewrite_axis_style_signature

from . import variables
from .meta import License, Source, TableMeta, VariableMeta

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
        :param underscore: Underscore table columns and indexes. See `underscore_table` for help
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
            from .utils import underscore_table

            underscore_table(self, inplace=True, camel_to_snake=camel_to_snake)

        # reuse metadata from a different table
        if like is not None:
            self.copy_metadata_from(like)

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

        # If each variable does not have sources, load them from the dataset.
        # TODO: I think this is not a good idea, consider removing.
        # table = assign_dataset_sources_and_licenses_to_each_variable(table=table)

        # Add processing log to the metadata of each variable in the table.
        # TODO: For some reason, the snapshot loading entry gets repeated.
        table = update_processing_logs_when_loading_or_creating_table(table=table)

        return table

    # Mypy complaints about this not matching the defintiion of NDFrame.to_csv but I don't understand why
    def to_csv(self, path: Any, **kwargs: Any) -> None:  # type: ignore
        """
        Save this table as a csv file plus accompanying JSON metadata file.
        If the table is stored at "mytable.csv", the metadata will be at
        "mytable.meta.json".
        """
        if not isinstance(path, str) or not path.endswith(".csv"):
            raise ValueError(f'filename must end in ".csv": {path}')

        df = pd.DataFrame(self)
        # if the dataframe uses the default index then we don't want to store it (would be a column of row numbers)
        save_index = self.primary_key != []
        df.to_csv(path, index=save_index, **kwargs)

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
        if not isinstance(path, str) or not path.endswith(".feather"):
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
        if not isinstance(path, str) or not path.endswith(".parquet"):
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
                    variables.update_variable_name(variable=value, name=key)
                self._fields[key] = value.metadata
                # TODO: The only reason why we have to check if variables.PROCESSING_LOG is true is the test
                #   "test_field_access_can_be_typecast". Consider adapting the test and then remove this check.
                if variables.PROCESSING_LOG and len(value.metadata.processing_log) > 0:
                    # If a new variable is added to a table, check its last entry in the processing log.
                    # If the last variable name is different to the name of the new column, add an entry to the log,
                    # stating that the variable has changed name (from the old to the current one).
                    last_variable_name = value.metadata.processing_log[-1]["variable"]
                    if last_variable_name != key:
                        value.update_log(
                            parents=[last_variable_name], operation="rename", variable_name=key, inplace=True
                        )
            else:
                self._fields[key] = VariableMeta()

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

        if inplace:
            new_table = self

        # construct new _fields attribute
        fields = {}
        for old_col, new_col in zip(old_cols, new_table.all_columns):
            if inplace:
                fields[new_col] = self._fields[old_col]
            else:
                fields[new_col] = copy.deepcopy(self._fields[old_col])

            # Update processing log.
            if old_col != new_col:
                fields[new_col].processing_log = variables.add_entry_to_processing_log(
                    processing_log=fields[new_col].processing_log,
                    variable_name=new_col,
                    parents=[old_col],
                    operation="rename",
                )

        new_table._fields = defaultdict(VariableMeta, fields)

        if inplace:
            return None
        else:
            return cast(Table, new_table)

    @property
    def all_columns(self) -> List[str]:
        "Return names of all columns in the dataset, including the index."
        combined: List[str] = filter(None, list(self.index.names) + list(self.columns))  # type: ignore
        return combined

    def update_metadata_from_yaml(
        self, path: Union[Path, str], table_name: str, extra_variables: Literal["raise", "ignore"] = "raise"
    ) -> None:
        """Update metadata of table and variables from a YAML file.
        :param path: Path to YAML file.
        :param table_name: Name of table, also updates this in the metadata.
        """
        from .meta import DatasetMeta
        from .utils import dynamic_yaml_load

        annot = dynamic_yaml_load(path, DatasetMeta._params_yaml(self.metadata.dataset or DatasetMeta()))

        self.metadata.short_name = table_name

        t_annot = annot["tables"][table_name]

        # validation
        if extra_variables == "raise":
            yaml_variable_names = t_annot.get("variables", {}).keys()
            table_variable_names = self.columns
            extra_variable_names = yaml_variable_names - table_variable_names
            if extra_variable_names:
                raise ValueError(f"Table {table_name} has extra variables: {extra_variable_names}")

        # update variables
        for v_short_name, v_annot in (t_annot.get("variables", {}) or {}).items():
            if v_short_name in self.columns:
                for k, v in v_annot.items():
                    # create an object out of sources
                    if k == "sources":
                        self[v_short_name].metadata.sources = [Source(**source) for source in v]
                    else:
                        setattr(self[v_short_name].metadata, k, v)

        # update table attributes
        for k, v in t_annot.items():
            if k != "variables":
                setattr(self.metadata, k, v)

    def prune_metadata(self) -> "Table":
        """Prune metadata for columns that are not in the table. This can happen after slicing
        the table by columns."""
        self._fields = {col: self._fields[col] for col in self.all_columns}
        return self

    def copy(self, deep: bool = True) -> "Table":
        """Copy table together with all its metadata."""
        tab = super().copy(deep=deep)
        tab.copy_metadata_from(self)
        return tab

    def copy_metadata_from(self, table: "Table", errors: Literal["raise", "ignore", "warn"] = "raise") -> None:
        """Copy metadata from a different table to self."""
        self.metadata = dataclasses.replace(table.metadata)

        extra_columns = set(table.columns) - set(self.columns)
        missing_columns = set(self.columns) - set(table.columns)
        common_columns = set(self.columns) & set(table.columns)

        if errors == "raise":
            if extra_columns:
                raise ValueError(f"Extra columns in table: {extra_columns}")
            if missing_columns:
                raise ValueError(f"Missing columns in table: {missing_columns}")
        elif errors == "warn":
            if extra_columns:
                log.warning(f"Extra columns in table: {extra_columns}")
            if missing_columns:
                log.warning(f"Missing columns in table: {missing_columns}")

        # NOTE: copying with `dataclasses.replace` is much faster than `copy.deepcopy`
        new_fields = defaultdict(VariableMeta)
        for k in common_columns:
            # copy if we have metadata in the other table
            if k in table._fields:
                v = table._fields[k]
                new_fields[k] = dataclasses.replace(v)
                new_fields[k].sources = [dataclasses.replace(s) for s in v.sources]
            # otherwise keep current metadata (if it exists)
            elif k in self._fields:
                new_fields[k] = self._fields[k]
        self._fields = new_fields

    @overload
    def set_index(
        self,
        keys: Union[str, List[str]],
        *,
        inplace: Literal[True],
    ) -> None:
        ...

    @overload
    def set_index(self, keys: Union[str, List[str]], *, inplace: Literal[False]) -> "Table":
        ...

    @overload
    def set_index(self, keys: Union[str, List[str]]) -> "Table":
        ...

    def set_index(
        self,
        keys: Union[str, List[str]],
        **kwargs,
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

        t.copy_metadata_from(self, errors="ignore")

        # copy variables metadata from other table
        if isinstance(other, Table):
            for k, v in other._fields.items():
                t._fields[k] = dataclasses.replace(v)
                t._fields[k].sources = [dataclasses.replace(s) for s in v.sources]
        return t  # type: ignore

    def _repr_html_(self):
        html = super()._repr_html_()
        return """
             <h2 style="margin-bottom: 0em"><pre>{}</pre></h2>
             <p style="font-variant: small-caps; font-size: 1.5em; font-family: sans-serif; color: grey; margin-top: -0.2em; margin-bottom: 0.2em">table</p>
             {}
        """.format(
            self.metadata.short_name, html
        )

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

    def underscore(self, **kwargs) -> "Table":
        from .utils import underscore_table

        return underscore_table(self, inplace=False, **kwargs)

    def dropna(self, *args, **kwargs) -> "Table":
        tb = super().dropna(*args, **kwargs).copy()
        for column in list(tb.all_columns):
            tb._fields[column].processing_log = variables.add_entry_to_processing_log(
                processing_log=tb._fields[column].processing_log,
                variable_name=column,
                parents=[column],
                operation="dropna",
            )

        return cast("Table", tb)

    def copy_metadata(
        self, from_table: "Table", include_missing_variables: bool = False, inplace: bool = False
    ) -> Optional["Table"]:
        return copy_metadata(
            to_table=self, from_table=from_table, include_missing_variables=include_missing_variables, inplace=inplace
        )

    def update_log(
        self,
        operation: str,
        parents: Optional[List[Any]] = None,
        variable_names: Optional[List[str]] = None,
        comment: Optional[str] = None,
        inplace: bool = False,
    ) -> Optional["Table"]:
        return update_log(
            table=self,
            operation=operation,
            parents=parents,
            variable_names=variable_names,
            comment=comment,
            inplace=inplace,
        )

    def amend_log(
        self,
        operation: str,
        parents: Optional[List[Any]] = None,
        variable_names: Optional[List[str]] = None,
        comment: Optional[str] = None,
        entry_num: Optional[int] = -1,
        inplace: bool = False,
    ) -> Optional["Table"]:
        return amend_log(
            table=self,
            operation=operation,
            parents=parents,
            variable_names=variable_names,
            comment=comment,
            entry_num=entry_num,
            inplace=inplace,
        )

    def sort_values(self, by: str, *args, **kwargs) -> "Table":
        tb = super().sort_values(by=by, *args, **kwargs).copy()
        for column in list(tb.all_columns):
            tb._fields[column].processing_log = variables.add_entry_to_processing_log(
                processing_log=tb._fields[column].processing_log, variable_name=column, parents=[by], operation="sort"
            )

        return cast("Table", tb)


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
        # "column" is a tuple with all column index levels.
        # For now, I assume the only metadata we want to propagate is the one of the upper level.
        # Alternatively, we could combine the metadata of the upper level variable with the metadata of the original
        # variable of all subsequent levels.
        column_metadata = variables.combine_variables_metadata(
            variables=variables_to_combine, operation="pivot", name=column_name
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


def _add_table_and_variables_metadata_to_table(table: Table, metadata: Optional[TableMeta]) -> Table:
    if metadata is not None:
        table.metadata = metadata
        for column in list(table.all_columns):
            table._fields[column].sources = metadata.dataset.sources  # type: ignore
            table._fields[column].licenses = metadata.dataset.licenses  # type: ignore
    table = update_processing_logs_when_loading_or_creating_table(table=table)

    return table


def read_csv(
    filepath_or_buffer: Union[str, Path, IO[AnyStr]],
    metadata: Optional[TableMeta] = None,
    underscore: bool = False,
    *args,
    **kwargs,
) -> Table:
    table = Table(pd.read_csv(filepath_or_buffer=filepath_or_buffer, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata)
    if isinstance(filepath_or_buffer, (str, Path)):
        table = update_log(table=table, operation="load", parents=[filepath_or_buffer])
    else:
        log.warning("Currently, the processing log cannot be updated unless you pass a path to read_csv.")

    return cast(Table, table)


def read_excel(
    io: Union[str, Path], *args, metadata: Optional[TableMeta] = None, underscore: bool = False, **kwargs
) -> Table:
    table = Table(pd.read_excel(io=io, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata)
    # Note: Maybe we should include the sheet name in parents.
    table = update_log(table=table, operation="load", parents=[io], inplace=False)

    return cast(Table, table)


class ExcelFile(pd.ExcelFile):
    def parse(
        self,
        sheet_name: Union[str, int] = 0,
        *args,
        metadata: Optional[TableMeta] = None,
        underscore: bool = False,
        **kwargs,
    ):
        # Note: Maybe we should include the sheet name in parents.
        df = super().parse(sheet_name=sheet_name, *args, **kwargs)  # type: ignore
        table = Table(df, underscore=underscore, short_name=str(sheet_name))
        if metadata is not None:
            table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata)
        # Note: Maybe we should include the sheet name in parents.
        table = update_log(table=table, operation="load", parents=[self.io], inplace=False)

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

    table = _add_processing_log_entry_to_each_variable(table=table, parents=parents, operation=operation)

    return table


def update_processing_logs_when_saving_table(table: Table, path: Union[str, Path]) -> Table:
    # Infer the ETL uri from the path where the table will be saved.
    # Note: If the path does not fit the expected format, the result will be an arbitrary path, but it will not raise an
    # error, as long as path is a Path.
    path = Path(path)
    uri = "/".join(path.absolute().parts[-5:-1] + tuple([path.stem]))
    table = _add_processing_log_entry_to_each_variable(table=table, parents=[uri], operation="save")

    return table


def _add_processing_log_entry_to_each_variable(
    table: Table, parents: List[Any], operation: variables.OPERATION
) -> Table:
    # Add a processing log entry to each column, including index columns.
    for column in list(table.all_columns):
        # New entry to add to the processing log.
        # Note: The function add_entry_to_processing_log receives the argument variable_name, but in the processing log,
        # the entry has the field "variable" (for simplicity).
        log_new_entry = {"variable_name": column, "parents": parents, "operation": operation}

        if log_new_entry not in table._fields[column].processing_log:
            # If the processing log is not empty but the last entry is identical to the one we want to insert, skip, to
            # avoid storing the same entry multiple times.
            # This happens for example when saving tables, given that tables are stored in different formats.
            # Otherwise, append a new entry to the processing log.
            table._fields[column].processing_log = variables.add_entry_to_processing_log(
                processing_log=table._fields[column].processing_log, **log_new_entry
            )

    return table


def assign_dataset_sources_and_licenses_to_each_variable(table: Table) -> Table:
    # Get sources and licenses from the table dataset.
    sources = []
    licenses = []
    if hasattr(table.metadata, "dataset") and hasattr(table.metadata.dataset, "sources"):
        sources = table.metadata.dataset.sources  # type: ignore
    if hasattr(table.metadata, "dataset") and hasattr(table.metadata.dataset, "sources"):
        licenses = table.metadata.dataset.licenses  # type: ignore

    if len(sources) == len(licenses) == 0:
        # There are no default sources/licenses to assign to each variable.
        return table

    # If a variable does not have sources/licenses defined, assign the ones from the dataset.
    # Do this for all columns, including index columns.
    for column in list(table.all_columns):
        if len(table._fields[column].sources) == 0:
            table._fields[column].sources = sources
        if len(table._fields[column].licenses) == 0:
            table._fields[column].licenses = licenses

    return table


@overload
def copy_metadata(
    from_table: Table, to_table: Table, include_missing_variables: bool = False, inplace: bool = False
) -> Table:
    ...


@overload
def copy_metadata(
    from_table: Table, to_table: Table, include_missing_variables: bool = False, inplace: bool = True
) -> None:
    ...


def copy_metadata(
    from_table: Table, to_table: Table, include_missing_variables: bool = False, inplace: bool = False
) -> Optional[Table]:
    if not inplace:
        to_table = to_table.copy()

    # Copy the table metadata.
    to_table.metadata = copy.deepcopy(from_table.metadata)

    if include_missing_variables:
        # Copy metadata from all variables (in case you may need them later).
        to_table._fields = copy.deepcopy(from_table._fields)
    else:
        # Find variables in the destination table that had metadata in the reference table.
        existing_variables = set(to_table.all_columns) & set(from_table._fields.keys())
        # Copy the metadata of those variables from the reference table to the destination table.
        to_table._fields = copy.deepcopy(
            defaultdict(VariableMeta, {variable: from_table._fields[variable] for variable in existing_variables})
        )

    if not inplace:
        return to_table


@overload
def update_log(
    table: Table,
    operation: str,
    parents: Optional[List[Any]] = None,
    variable_names: Optional[List[str]] = None,
    comment: Optional[str] = None,
    inplace: bool = True,
) -> None:
    ...


@overload
def update_log(
    table: Table,
    operation: str,
    parents: Optional[List[Any]] = None,
    variable_names: Optional[List[str]] = None,
    comment: Optional[str] = None,
    inplace: bool = False,
) -> Table:
    ...


def update_log(
    table: Table,
    operation: str,
    parents: Optional[List[Any]] = None,
    variable_names: Optional[List[str]] = None,
    comment: Optional[str] = None,
    inplace: bool = False,
) -> Optional[Table]:
    if not inplace:
        table = table.copy()

    # Append a new entry to the processing log of the required variables.
    if variable_names is None:
        # If no variable is specified, assume all (including index columns).
        variable_names = list(table.all_columns)
    for column in variable_names:
        # If parents is not defined, assume the parents are simply the current variable.
        _parents = parents or [column]
        # Update (in place) the processing log of current variable.
        table._fields[column].processing_log = variables.add_entry_to_processing_log(
            processing_log=table._fields[column].processing_log,
            variable_name=column,
            parents=_parents,
            operation=operation,
            comment=comment,
        )

    if not inplace:
        return table


def amend_log(
    table: Table,
    operation: str,
    parents: Optional[List[Any]] = None,
    variable_names: Optional[List[str]] = None,
    comment: Optional[str] = None,
    entry_num: Optional[int] = -1,
    inplace: bool = False,
) -> Optional[Table]:
    if not inplace:
        table = table.copy()

    # Append a new entry to the processing log of the required variables.
    if variable_names is None:
        # If no variable is specified, assume all (including index columns).
        variable_names = list(table.all_columns)
    for column in variable_names:
        # If parents is not defined, assume the parents are simply the current variable.
        _parents = parents or [column]
        # Update (in place) the processing log of current variable.
        table._fields[column].processing_log = variables.amend_entry_in_processing_log(
            processing_log=table._fields[column].processing_log,
            variable_name=column,
            parents=_parents,
            operation=operation,
            comment=comment,
            entry_num=entry_num,
        )

    if not inplace:
        return table


def get_unique_sources_from_tables(tables: List[Table]) -> List[Source]:
    # Make a list of all sources of all variables in all tables.
    sources = sum([table._fields[column].sources for table in tables for column in list(table.all_columns)], [])

    # Get unique array of tuples of source fields (respecting the order).
    unique_sources_array = pd.unique([tuple(source.to_dict().items()) for source in sources])

    # Make a list of unique sources.
    unique_sources = [Source.from_dict(dict(source)) for source in unique_sources_array]  # type: ignore

    return unique_sources


def get_unique_licenses_from_tables(tables: List[Table]) -> List[License]:
    # Make a list of all licenses of all variables in all tables.
    licenses = sum([table._fields[column].licenses for table in tables for column in list(table.all_columns)], [])

    # Get unique array of tuples of license fields (respecting the order).
    unique_licenses_array = pd.unique([tuple(license.to_dict().items()) for license in licenses])

    # Make a list of unique licenses.
    unique_licenses = [License.from_dict(dict(license)) for license in unique_licenses_array]  # type: ignore

    return unique_licenses


def _combine_tables_titles_and_descriptions(tables: List[Table], title_or_description: str) -> Optional[str]:
    # Keep the title only if all tables have exactly the same title.
    # Otherwise we assume that the table has a different meaning, and its title should be manually handled.
    title_or_description_combined = None
    titles_or_descriptions = pd.unique([getattr(table.metadata, title_or_description) for table in tables])
    if len(titles_or_descriptions) == 1:
        title_or_description_combined = titles_or_descriptions[0]

    return title_or_description_combined


def combine_tables_titles(tables: List[Table]) -> Optional[str]:
    return _combine_tables_titles_and_descriptions(tables=tables, title_or_description="title")


def combine_tables_descriptions(tables: List[Table]) -> Optional[str]:
    return _combine_tables_titles_and_descriptions(tables=tables, title_or_description="description")


def combine_tables_metadata(tables: List[Table], short_name: Optional[str] = None) -> TableMeta:
    title = combine_tables_titles(tables=tables)
    description = combine_tables_descriptions(tables=tables)
    if short_name is None:
        # If a short name is not specified, take it from the first table.
        short_name = tables[0].metadata.short_name
    metadata = TableMeta(title=title, description=description, short_name=short_name)

    return metadata


def check_all_variables_have_metadata(tables: List[Table], fields: Optional[List[str]] = None) -> None:
    if fields is None:
        fields = ["sources", "licenses"]

    for table in tables:
        table_name = table.metadata.short_name
        for column in table.columns:
            for field in fields:
                if not getattr(table[column].metadata, field):
                    log.warning(f"Table {table_name}, column {column} has no {field}.")

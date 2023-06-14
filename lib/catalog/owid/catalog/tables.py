#
#  tables.py
#

import copy
import dataclasses
import json
from collections import defaultdict
from os.path import dirname, join, splitext
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Union, cast, overload

import pandas as pd
import pyarrow
import pyarrow.parquet as pq
import requests
import structlog
import yaml
from owid.repack import repack_frame
from pandas._typing import FilePath, ReadCsvBuffer  # type: ignore
from pandas.util._decorators import rewrite_axis_style_signature

from . import variables
from .meta import Source, TableMeta, VariableMeta
from .variables import UPDATE_PROCESSING_LOG

log = structlog.get_logger()

SCHEMA = json.load(open(join(dirname(__file__), "schemas", "table.json")))
METADATA_FIELDS = list(SCHEMA["properties"])


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
        like: Optional["Table"] = None,
        **kwargs: Any,
    ) -> None:
        """
        :param metadata: TableMeta to use
        :param short_name: Use empty TableMeta and fill it with `short_name`. This is a shorter version
            of `Table(df, metadata=TableMeta(short_name="my_name"))`
        :param underscore: Underscore table columns and indexes. See `underscore_table` for help
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

            underscore_table(self, inplace=True)

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
        if UPDATE_PROCESSING_LOG:
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
        table = assign_dataset_sources_and_licenses_to_each_variable(table=table)

        if UPDATE_PROCESSING_LOG:
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
            else:
                self._fields[key] = VariableMeta()

    def equals_table(self, rhs: "Table") -> bool:
        return isinstance(rhs, Table) and self.metadata == rhs.metadata and self.to_dict() == rhs.to_dict()

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

            if UPDATE_PROCESSING_LOG:
                # Update processing log.
                if old_col != new_col:
                    fields[new_col].processing_log = variables.add_entry_to_processing_log(
                        processing_log=fields[new_col].processing_log,
                        variable=new_col,
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
        with open(path) as istream:
            annot = yaml.safe_load(istream)

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

    def merge(self, right, *args, **kwargs) -> "Table":
        return merge(left=self, right=right, *args, **kwargs)

    def melt(
        self,
        id_vars: Optional[Union[Tuple[str], List[str], str]] = None,
        value_vars: Optional[Union[Tuple[str], List[str], str]] = None,
        var_name: str = "variable",
        value_name: str = "value",
        *args,
        **kwargs,
    ) -> "Table":
        return melt(
            frame=self,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name,
            *args,
            **kwargs,
        )

    def pivot(self, *args, **kwargs) -> "Table":
        return pivot(data=self, *args, **kwargs)

    def underscore(self) -> "Table":
        from .utils import underscore_table

        return underscore_table(self, inplace=False)


def merge(left, right, *args, **kwargs) -> Table:
    # TODO: This function needs further logic. For example, to handle "on"/"left_on"/"right_on" columns,
    #  or suffixes, or overlapping columns (that will end in "_x" and "_y" by default), or indexes.
    tb = Table(pd.merge(left, right, *args, **kwargs))
    columns_that_were_in_left = set(tb.columns) & set(left.columns)
    columns_that_were_in_right = set(tb.columns) & set(right.columns)

    for column in columns_that_were_in_left:
        tb[column].metadata = variables.combine_variables_metadata([left[column]], operation="merge")
    for column in columns_that_were_in_right:
        tb[column].metadata = variables.combine_variables_metadata([right[column]], operation="merge")

    return tb


def concat(objs: List[Table], *, axis: int = 0, join: str = "outer", ignore_index: bool = False, **kwargs) -> Table:
    # TODO: Add more logic to this function to handle indexes and possibly other arguments.
    table = Table(pd.concat(objs=objs, axis=axis, join=join, ignore_index=ignore_index, **kwargs))  # type: ignore

    if axis == 1:
        # Assign to each variable its original metadata.
        original_variables = [table_i[column] for table_i in objs for column in table_i]
        for i, column in enumerate(table.columns):
            assert column == original_variables[i].name
            table[column].metadata = original_variables[i].metadata
            table[column].metadata.processing_log = variables.add_entry_to_processing_log(
                processing_log=table[column].metadata.processing_log,
                variable=column,
                parents=[column],
                operation="concat",
            )
    elif axis == 0:
        # Add to each column either the metadata of the original variable (if the variable appeared only in one of the input
        # tables) or the combination of the metadata from different tables (if the variable appeared in various tables).
        for column in table.columns:
            variables_to_combine = [table_i[column] for table_i in objs if column in table_i.columns]
            table[column].metadata = variables.combine_variables_metadata(
                variables=variables_to_combine, operation="concat", name=column
            )

    return table


def melt(
    frame: Table,
    id_vars: Optional[Union[Tuple[str], List[str], str]] = None,
    value_vars: Optional[Union[Tuple[str], List[str], str]] = None,
    var_name: str = "variable",
    value_name: str = "value",
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

    # Copy the original table metadata to the new table.
    table.metadata = copy.deepcopy(frame.metadata)

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
            variables=[frame[var] for var in id_vars_list], operation="melt", name=variable
        )

    return table


# TODO: Handle metadata and processing info for each of the following functions.
def pivot(*args, **kwargs) -> Table:
    return Table(pd.pivot(*args, **kwargs))


def _add_table_and_variables_metadata_to_table(table: Table, metadata: Optional[TableMeta]) -> Table:
    if metadata is not None:
        table.metadata = metadata
        for column in table.columns:
            table[column].metadata.sources = metadata.dataset.sources  # type: ignore
            table[column].metadata.licenses = metadata.dataset.licenses  # type: ignore
    if UPDATE_PROCESSING_LOG:
        table = update_processing_logs_when_loading_or_creating_table(table=table)

    return table


def read_csv(
    filepath_or_buffer: Union[FilePath, ReadCsvBuffer[bytes], ReadCsvBuffer[str]],
    metadata: Optional[TableMeta] = None,
    underscore: bool = False,
    *args,
    **kwargs,
) -> Table:
    table = Table(pd.read_csv(filepath_or_buffer=filepath_or_buffer, *args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata)
    return table


def read_excel(*args, metadata: Optional[TableMeta] = None, underscore: bool = False, **kwargs) -> Table:
    table = Table(pd.read_excel(*args, **kwargs), underscore=underscore)
    table = _add_table_and_variables_metadata_to_table(table=table, metadata=metadata)
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
    # Get the names of the columns currently used for the index of the table (if any).
    index_columns = table.metadata.primary_key

    # If the table has an index, reset it so we have access to all variables in the table.
    if len(index_columns) > 0:
        table = table.reset_index(drop=False)  # type: ignore
    for column in table.columns:
        # New entry to add to the processing log.
        log_new_entry = {"variable": column, "parents": parents, "operation": operation}

        if table[column].metadata.processing_log is None:
            # If no processing log is found for a variable, start a new one.
            table[column].metadata.processing_log = []

        if log_new_entry in table[column].metadata.processing_log:
            # If the processing log is not empty but the last entry is identical to the one we want to insert, skip, to
            # avoid storing the same entry multiple times.
            # This happens for example when saving tables, given that tables are stored in different formats.
            pass
        else:
            # Append a new entry to the processing log.
            table[column].metadata.processing_log = variables.add_entry_to_processing_log(
                processing_log=table[column].metadata.processing_log, **log_new_entry
            )
    if len(index_columns) > 0:
        table = table.set_index(index_columns)

    return table


def assign_dataset_sources_and_licenses_to_each_variable(table: Table) -> Table:
    # Get the names of the columns currently used for the index of the table (if any).
    index_columns = table.metadata.primary_key

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

    # If the table has an index, reset it so we have access to all variables in the table.
    if len(index_columns) > 0:
        table = table.reset_index(drop=False)  # type: ignore

    # If a variable does not have sources/licenses defined, assign the ones from the dataset.
    for column in table.columns:
        if len(table[column].metadata.sources) == 0:
            table[column].metadata.sources = sources
        if len(table[column].metadata.licenses) == 0:
            table[column].metadata.licenses = licenses

    # Set the original index to the table.
    if len(index_columns) > 0:
        table = table.set_index(index_columns)

    return table

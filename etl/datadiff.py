import difflib
import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Union, cast

import numpy as np
import pandas as pd
import requests
import rich
import rich_click as click
import structlog
from owid.catalog import Dataset, DatasetMeta, LocalCatalog, RemoteCatalog, Table, VariableMeta, find
from owid.catalog.catalogs import CHANNEL, OWID_CATALOG_URI
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax

from etl.files import yaml_dump
from etl.steps import load_dag
from etl.tempcompare import series_equals

log = structlog.get_logger()


class DatasetError(Exception):
    pass


class DatasetDiff:
    """Compare two datasets and print a summary of the differences."""

    def __init__(
        self,
        ds_a: Optional[Dataset],
        ds_b: Optional[Dataset],
        verbose: bool = False,
        cols: Optional[str] = None,
        print: Callable = rich.print,
        snippet: bool = False,
    ):
        """
        :param cols: Only compare columns matching pattern
        :param print: Function to print the diff summary. Defaults to rich.print.
        :param snippet: Print snippet for loading both tables
        """
        assert ds_a or ds_b, "At least one Dataset must be provided"
        self.ds_a = ds_a
        self.ds_b = ds_b
        self.p = print
        self.verbose = verbose
        self.cols = cols
        self.snippet = snippet

    def _diff_datasets(self, ds_a: Optional[Dataset], ds_b: Optional[Dataset]):
        if ds_a and ds_b:
            ds_short_name = ds_a.metadata.short_name
            assert ds_short_name

            new_version = " (new version)" if ds_a.metadata.version != ds_b.metadata.version else ""

            # compare dataset metadata
            diff = _dict_diff(_dataset_metadata_dict(ds_a), _dataset_metadata_dict(ds_b), tabs=2)
            if diff:
                self.p(f"[yellow]~ Dataset [b]{dataset_uri(ds_b)}[/b]{new_version}")
                if self.verbose:
                    self.p(diff)
            else:
                self.p(f"[white]= Dataset [b]{dataset_uri(ds_b)}{new_version}[/b]")
        elif ds_a:
            self.p(f"[red]- Dataset [b]{dataset_uri(ds_a)}[/b]")
        elif ds_b:
            self.p(f"[green]+ Dataset [b]{dataset_uri(ds_b)}[/b]")
            for table_name in ds_b.table_names:
                self.p(f"\t[green]+ Table [b]{table_name}[/b]")
                for col in ds_b[table_name].columns:
                    self.p(f"\t\t[green]+ Column [b]{col}[/b]")

    def _snippet(self, ds_a: Dataset, ds_b: Dataset, table_name: str) -> Panel:
        """Print code for loading both tables."""

        def _snippet_dataset(ds: Dataset, table_name: str) -> str:
            m = ds.metadata
            if isinstance(ds, RemoteDataset):
                return f'RemoteCatalog(channels=["{m.channel}"]).find_one(table="{table_name}", dataset="{m.short_name}", version="{m.version}", namespace="{m.namespace}", channel="{m.channel}")'
            else:
                return f'Dataset(DATA_DIR / "{m.uri}")["{table_name}"]'

        code = f"""
from owid.catalog import RemoteCatalog, Dataset
from etl.paths import DATA_DIR

ta = {_snippet_dataset(ds_a, table_name)}
tb = {_snippet_dataset(ds_b, table_name)}
""".strip()

        syntax = Syntax(code, "python", theme="monokai")
        return Panel(syntax, title="Python Code", border_style="blue")

    def _diff_tables(self, ds_a: Dataset, ds_b: Dataset, table_name: str):
        if self.snippet:
            self.p(self._snippet(ds_a, ds_b, table_name))

        if table_name not in ds_b.table_names:
            self.p(f"\t[red]- Table [b]{table_name}[/b]")
            for col in ds_a[table_name].columns:
                self.p(f"\t\t[red]- Column [b]{col}[/b]")
        elif table_name not in ds_a.table_names:
            self.p(f"\t[green]+ Table [b]{table_name}[/b]")
            for col in ds_b[table_name].columns:
                self.p(f"\t\t[green]+ Column [b]{col}[/b]")
        else:
            # get both tables in parallel
            with ThreadPoolExecutor() as executor:
                future_a = executor.submit(ds_a.__getitem__, table_name)
                future_b = executor.submit(ds_b.__getitem__, table_name)

                table_a = future_a.result()
                table_b = future_b.result()

            # set default index for datasets that don't have one
            if table_a.index.names == [None] and table_b.index.names == [None]:
                candidates = {"entity", "date", "country", "year"}
                new_index_cols = list(candidates & set(table_a.columns) & set(table_b.columns))
                if new_index_cols:
                    table_a = table_a.set_index(new_index_cols)
                    table_b = table_b.set_index(new_index_cols)

            # if using default index, it is possible that we have non-determinstic order
            # try sorting by the first two columns
            if (
                table_a.index.names == [None]
                and table_b.index.names == [None]
                and len(table_a) == len(table_b)
                and table_a.index[-1] == len(table_a) - 1
                and len(table_a) <= 1000
            ):
                table_a = table_a.sort_values(list(table_a.columns)).reset_index(drop=True)
                table_b = table_b.sort_values(list(table_b.columns)).reset_index(drop=True)

            # indexes differ, reset them to make them somehow comparable
            if table_a.index.names != table_b.index.names:
                if table_a.index.names != [None]:
                    table_a = table_a.reset_index()
                if table_b.index.names != [None]:
                    table_b = table_b.reset_index()

            # only sort index if different to avoid unnecessary sorting for huge datasets such as ghe
            if len(table_a) != len(table_b) or not _index_equals(table_a, table_b):
                table_a, table_b, eq_index, new_index, removed_index = _align_tables(table_a, table_b)
            else:
                eq_index = pd.Series(True, index=table_a.index)
                new_index = pd.Series(False, index=table_a.index)
                removed_index = pd.Series(False, index=table_a.index)

            # resetting index will make comparison easier
            dims = [dim for dim in table_a.index.names if dim is not None]
            table_a: Table = table_a.reset_index()
            table_b: Table = table_b.reset_index()
            eq_index = cast(pd.Series, eq_index.reset_index(drop=True))
            new_index = cast(pd.Series, new_index.reset_index(drop=True))
            removed_index = cast(pd.Series, removed_index.reset_index(drop=True))

            # compare table metadata
            diff = _dict_diff(_table_metadata_dict(table_a), _table_metadata_dict(table_b), tabs=3)
            if diff:
                self.p(f"\t[yellow]~ Table [b]{table_name}[/b] (changed [u]metadata[/u])")

                if self.verbose:
                    self.p(diff)
            else:
                self.p(f"\t[white]= Table [b]{table_name}[/b]")

            # compare index
            if not eq_index.all():
                for dim in dims:
                    if eq_index.all():
                        self.p(f"\t\t[white]= Dim [b]{dim}[/b]")
                    else:
                        self.p(f"\t\t[yellow]~ Dim [b]{dim}[/b]")
                        if self.verbose:
                            dims_without_dim = [d for d in dims if d != dim]
                            out = _data_diff(
                                table_a,
                                table_b,
                                dim,
                                dims_without_dim,
                                eq_index,
                                eq_index,
                                new_index,
                                removed_index,
                                tabs=4,
                            )
                            if out:
                                self.p(out)

            # compare columns
            all_cols = sorted((set(table_a.columns) | set(table_b.columns)) - set(dims))
            for col in all_cols:
                if self.cols and not re.search(self.cols, col):
                    continue

                if col not in table_a.columns:
                    self.p(f"\t\t[green]+ Column [b]{col}[/b]")
                elif col not in table_b.columns:
                    self.p(f"\t\t[red]- Column [b]{col}[/b]")
                else:
                    col_a = table_a[col]
                    col_b = table_b[col]

                    # metadata diff
                    meta_diff = _dict_diff(
                        _column_metadata_dict(col_a.metadata), _column_metadata_dict(col_b.metadata), tabs=4
                    )

                    # equality on index and series
                    eq_data = series_equals(table_a[col], table_b[col])

                    changed = []
                    if meta_diff:
                        changed.append("changed [u]metadata[/u]")
                    if new_index.any():
                        changed.append("new [u]data[/u]")
                    if (~eq_data[~new_index]).any():
                        changed.append("changed [u]data[/u]")

                    if changed:
                        self.p(f"\t\t[yellow]~ Column [b]{col}[/b] ({', '.join(changed)})")
                        if self.verbose:
                            if meta_diff:
                                self.p(meta_diff)
                            if new_index.any() or removed_index.any() or (~eq_data).any():
                                if meta_diff:
                                    self.p("")
                                out = _data_diff(
                                    table_a, table_b, col, dims, eq_data, eq_index, new_index, removed_index, tabs=4
                                )
                                if out:
                                    self.p(out)
                    else:
                        # do not print identical columns
                        pass

    def summary(self):
        """Print a summary of the differences between the two datasets."""
        self._diff_datasets(self.ds_a, self.ds_b)

        if self.ds_a and self.ds_b:
            for table_name in set(self.ds_a.table_names) | set(self.ds_b.table_names):
                self._diff_tables(self.ds_a, self.ds_b, table_name)


class RemoteDataset:
    """Dataset from remote catalog with the same interface as Dataset."""

    def __init__(self, dataset_meta: DatasetMeta, table_names: List[str]):
        self.metadata = dataset_meta
        self.table_names = table_names

    def __getitem__(self, name: str) -> Table:
        tables = find(
            table=name,
            namespace=self.metadata.namespace,
            version=str(self.metadata.version),
            dataset=self.metadata.short_name,
            channels=[self.metadata.channel],  # type: ignore
        )

        tables = tables[tables.channel == self.metadata.channel]  # type: ignore

        # find matches substrings, we have to further filter it
        tables = tables[tables.table == name]

        return tables.load()


@click.command(name="diff", help=__doc__)
@click.argument(
    "path-a",
    type=click.Path(),
)
@click.argument(
    "path-b",
    type=click.Path(),
)
@click.option(
    "--channel",
    "-c",
    multiple=True,
    type=click.Choice(CHANNEL.__args__),
    default=["garden", "meadow", "grapher"],
    help="Compare only selected channel (subfolder of data/).",
)
@click.option(
    "--include",
    type=str,
    help="Compare only datasets matching pattern.",
)
@click.option(
    "--cols",
    type=str,
    help="Compare only columns matching pattern.",
)
@click.option(
    "--exclude",
    "-e",
    type=str,
    help="Exclude datasets matching pattern.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Print more detailed differences.",
)
@click.option(
    "--snippet",
    is_flag=True,
    help="Print code snippet for loading both tables, useful for debugging in notebook",
)
@click.option(
    "--workers",
    "-w",
    type=int,
    help="Use multiple threads.",
    default=1,
)
def cli(
    path_a: str,
    path_b: str,
    channel: Iterable[CHANNEL],
    include: Optional[str],
    cols: Optional[str],
    exclude: Optional[str],
    verbose: bool,
    snippet: bool,
    workers: int,
) -> None:
    """Compare all datasets from two catalogs and print out a summary of their differences.

    Compare all the datasets from catalog in `PATH_A` with all the datasets in catalog `PATH_B`. The catalog paths link to the `data/` folder with all the datasets (it contains a `catalog.meta.json` file)

    You can also use a path to a dataset.

    Note that you can use the keyword "REMOTE" as the path, if you want to run a comparison with the remote catalog.

    This tool is useful as a quick way to see what has changed in the catalog and whether our updates don't have any unexpected side effects.

    **Note:** This command differs from `etl compare` in that it compares _all_ the datasets and not two specific ones.

    **How does it work?**

    It uses **source checksums** to find candidates for comparison. Source checksum includes all files used to generate the dataset and should be sufficient to find changed datasets, just note that we're not using checksum of the files themselves. So if you change core ETL code or some of the dependencies, e.g. change in owid-datautils-py, core ETL code or updating library version, the change won't be detected. In cases like these you should increment ETL version which is added to all source checksums (not implemented yet).

    **Example 1:** Compare the remote catalog with a local one

    ```
    $ etl diff REMOTE data/ --include maddison
    ```

    **Example 2:** Compare two local catalogs

    ```
    $ etl diff other-data/ data/ --include maddison
    ```
    """
    console = Console(tab_size=2)

    path_to_ds_a = _load_catalog_datasets(path_a, channel, include, exclude)
    path_to_ds_b = _load_catalog_datasets(path_b, channel, include, exclude)

    # only keep datasets in DAG
    dag_steps = {s.split("://")[1] for s in load_dag().keys()}
    path_to_ds_a = {k: v for k, v in path_to_ds_a.items() if k in dag_steps}
    path_to_ds_b = {k: v for k, v in path_to_ds_b.items() if k in dag_steps}

    any_diff = False
    any_error = False

    matched_datasets = []
    for path in sorted(set(path_to_ds_a.keys()) | set(path_to_ds_b.keys())):
        ds_a = _match_dataset(path_to_ds_a, path)
        ds_b = _match_dataset(path_to_ds_b, path)

        if ds_a and ds_b and ds_a.metadata.source_checksum == ds_b.metadata.source_checksum:
            # skip if they have the same source checksum, note that we're not comparing checksum of actual data
            # to improve performance. Source checksum should be enough
            continue

        matched_datasets.append((ds_a, ds_b))

    if workers > 1:
        futures = []

        with ThreadPoolExecutor(max_workers=workers) as executor:
            for ds_a, ds_b in matched_datasets:

                def func(ds_a, ds_b):
                    lines = []
                    differ = DatasetDiff(
                        ds_a, ds_b, cols=cols, print=lambda x: lines.append(x), verbose=verbose, snippet=snippet
                    )
                    differ.summary()
                    return lines

                futures.append(executor.submit(func, ds_a, ds_b))

            for future in futures:
                try:
                    lines = future.result()
                except DatasetError as e:
                    # soft fail and continue with another dataset
                    lines = [f"[bold red]⚠ Error: {e}[/bold red]"]
                except Exception as e:
                    # soft fail and continue with another dataset
                    log.error(e, exc_info=True)
                    any_error = True
                    lines = []
                    continue

                for line in lines:
                    console.print(line)

                    if "~" in line:
                        any_diff = True
    else:
        for ds_a, ds_b in matched_datasets:
            lines = []

            def _append_and_print(x):
                lines.append(x)
                console.print(x)

            try:
                differ = DatasetDiff(ds_a, ds_b, cols=cols, print=_append_and_print, verbose=verbose, snippet=snippet)
                differ.summary()
            except DatasetError as e:
                # soft fail and continue with another dataset
                _append_and_print(f"[bold red]⚠ Error: {e}[/bold red]")
                continue
            except Exception as e:
                # soft fail and continue with another dataset
                log.error(e, exc_info=True)
                any_error = True
                continue

            if any("~" in line for line in lines if isinstance(line, str)):
                any_diff = True

    console.print()
    if not path_to_ds_a and not path_to_ds_b:
        console.print("[yellow]❓ No datasets found[/yellow]")
    elif any_error:
        console.print("[bold red]⚠ Found errors, create an issue please[/bold red]")
    elif any_diff:
        console.print("[red]❌ Found differences[/red]")
    else:
        console.print("[green]✅ No differences found[/green]")
    console.print()

    console.print(
        "[b]Legend[/b]: [green]+New[/green]  [yellow]~Modified[/yellow]  [red]-Removed[/red]  [white]=Identical[/white]  [violet]Details[/violet]"
    )
    console.print(
        "[b]Hint[/b]: Run this locally with [cyan][b]etl diff REMOTE data/ --include yourdataset --verbose --snippet[/b][/cyan]"
    )
    console.print(
        "[b]Hint[/b]: Get detailed comparison with [cyan][b]compare --show-values channel namespace version short_name --values[/b][/cyan]"
    )
    exit(1 if any_diff else 0)


def _index_equals(table_a: pd.DataFrame, table_b: pd.DataFrame, sample: int = 1000) -> bool:
    """Check if two tables have the same index. Sample both tables to speed up the check."""
    if len(table_a) < sample and len(table_b) < sample:
        index_a = table_a.index
        index_b = table_b.index
    else:
        index_a = table_a.sample(sample, random_state=0, replace=True).index
        index_b = table_b.sample(sample, random_state=0, replace=True).index

    return index_a.equals(index_b)


def _dict_diff(dict_a: Dict[str, Any], dict_b: Dict[str, Any], tabs: int = 0, **kwargs) -> str:
    """Convert dictionaries into YAML and compare them using difflib. Return colored diff as a string."""
    meta_a = yaml_dump(dict_a, **kwargs)
    meta_b = yaml_dump(dict_b, **kwargs)

    lines = difflib.ndiff(meta_a.splitlines(keepends=True), meta_b.splitlines(keepends=True))  # type: ignore
    # do not print lines that are identical
    lines = [line for line in lines if not line.startswith("  ")]

    # add color
    lines = ["[violet]" + line for line in lines]

    if not lines:
        return ""
    else:
        # add tabs
        return "\t" * tabs + "".join(lines).replace("\n", "\n" + "\t" * tabs).rstrip()


def _df_to_str(df: pd.DataFrame, limit: int = 5) -> list[str]:
    lines = []
    if len(df) > limit:
        df_samp = df.sample(limit, random_state=0).sort_index()
    else:
        df_samp = df

    for line in df_samp.to_string(index=False).split("\n"):  # type: ignore
        lines.append("  " + line)
    return lines


def _data_diff(
    table_a: Table,
    table_b: Table,
    col: str,
    dims: list[str],
    eq_data: pd.Series,
    eq_index: pd.Series,
    new_index: pd.Series,
    removed_index: pd.Series,
    tabs: int = 0,
) -> str:
    """Return summary of data differences."""
    # eq = eq_data & eq_index
    n = (eq_index | new_index).sum()

    lines = []

    cols = [d for d in dims if d is not None] + [col]

    # new values
    if new_index.any():
        lines.append(
            f"+ New values: {new_index.sum()} / {n} ({new_index.sum() / n * 100:.2f}%)",
        )
        lines += _df_to_str(table_b.loc[new_index, cols])

    # removed values
    if removed_index.any():
        lines.append(
            f"- Removed values: {removed_index.sum()} / {n} ({removed_index.sum() / n * 100:.2f}%)",
        )
        lines += _df_to_str(table_a.loc[removed_index, cols])

    # changed values
    neq = ~eq_data & eq_index
    if neq.any():
        lines.append(
            f"~ Changed values: {neq.sum()} / {n} ({neq.sum() / n * 100:.2f}%)",
        )
        samp_a = table_a.loc[neq, cols]
        samp_b = table_b.loc[neq, cols]
        both = samp_a.merge(samp_b, on=dims, suffixes=(" -", " +"))
        lines += _df_to_str(both)

    # add color
    lines = ["[violet]" + line for line in lines]

    if not lines:
        return ""
    else:
        # add tabs
        return "\t" * tabs + "\n".join(lines).replace("\n", "\n" + "\t" * tabs).rstrip()

    """OLD CODE, PARTS OF IT COULD BE STILL USEFUL
    # changes in index
    for dim in dims:
        if dim is not None:
            diff_elements = table_a.loc[~eq, dim].dropna().astype(str).sort_values().unique().tolist()
            detail = f"{len(diff_elements)} affected" if len(diff_elements) > 5 else ", ".join(diff_elements)
            lines.append(f"- Dim `{dim}`: {detail}")

    lines.append(
        f"- Changed values: {(~eq).sum()} / {len(eq)} ({(~eq).sum() / len(eq) * 100:.2f}%)",
    )

    # changes in values
    if (
        table_a[col].dtype in ("category", "object", "string")
        or table_b[col].dtype in ("category", "object", "string")
        or _is_datetime(table_a[col].dtype)
    ):
        vals_a = set(table_a.loc[~eq, col].dropna().astype(str))
        vals_b = set(table_b.loc[~eq, col].dropna().astype(str))
        if vals_a - vals_b:
            lines.append(f"- Removed values: {', '.join(vals_a - vals_b)}")
        if vals_b - vals_a:
            lines.append(f"- New values: {', '.join(vals_b - vals_a)}")
    else:
        mean_a = table_a.loc[~eq, col].mean()
        mean_b = table_b.loc[~eq, col].mean()
        abs_diff = mean_b - mean_a
        mean = (mean_a + mean_b) / 2

        rel_diff = abs_diff / mean if not pd.isnull(mean) and mean != 0 else np.nan

        lines.append(f"- Avg. change: {abs_diff:.2f} ({rel_diff:.0%})")
    """


def _is_datetime(dtype: Any) -> bool:
    try:
        return np.issubdtype(dtype, np.datetime64)
    except Exception:
        return False


def _align_tables(table_a: Table, table_b: Table) -> tuple[Table, Table, pd.Series, pd.Series, pd.Series]:
    if not table_a.index.is_unique or not table_b.index.is_unique:
        raise DatasetError("Index must be unique.")

    if len(table_a.index.names) * len(table_a) >= 2 * 10**8:
        # table_a.align is very memory intensive for large tables as doesn't handle
        # categorical indexes well. We'd have to convert all categories to codes first,
        # align them and then convert back to categories.
        raise DatasetError("Cannot run datadiff for an index of such size.")

    table_a = _sort_index(table_a)
    table_b = _sort_index(table_b)

    # align tables by index
    table_a["_x"] = 1
    table_b["_x"] = 1
    table_a, table_b = table_a.align(table_b, join="outer", copy=False)

    new_index = table_a["_x"].isnull()
    removed_index = table_b["_x"].isnull()

    eq_index = ~(new_index | removed_index)
    table_a.drop(columns="_x", inplace=True)
    table_b.drop(columns="_x", inplace=True)

    return cast(Table, table_a), cast(Table, table_b), eq_index, new_index, removed_index


def _sort_index(df: Table) -> Table:
    """Sort dataframe by its index and make sure categories are sorted by their
    names and not codes. Modifies the dataframe in place and also returns it."""
    new_levels = []
    for level_name in df.index.names:
        level = df.index.get_level_values(level_name)
        if level.dtype == "category":
            level = level.reorder_categories(sorted(level.categories))
        new_levels.append(level)

    df.index = pd.MultiIndex.from_arrays(new_levels)
    df.sort_index(inplace=True)
    return df


def _match_dataset(path_to_ds: Dict[str, Any], path: str) -> Optional[Dataset]:
    """Get dataset from dictionary {path -> dataset}. Return dataset with the same version if available,
    otherwise return older version or None if there is no such dataset."""
    if path in path_to_ds:
        return path_to_ds[path]
    else:
        # find latest matching version
        channel, namespace, version, short_name = path.split("/")

        candidates = []
        for k in path_to_ds.keys():
            if re.match(f"{channel}/{namespace}/.*?/{short_name}", k):
                candidates.append(k)

        if candidates:
            latest_version = max(candidates)
            # make sure we don't compare to newer version
            if latest_version < path:
                return path_to_ds[latest_version]
            else:
                return None
        else:
            return None


def _load_catalog_datasets(
    catalog_path: str, channels: Iterable[CHANNEL], include: Optional[str], exclude: Optional[str]
) -> Dict[str, Any]:
    if catalog_path == "REMOTE":
        assert include, "You have to filter with --include when comparing with remote catalog"
        return _remote_catalog_datasets(channels=channels, include=include, exclude=exclude)
    else:
        return _local_catalog_datasets(catalog_path, channels=channels, include=include, exclude=exclude)


def _table_metadata_dict(tab: Table) -> Dict[str, Any]:
    """Extract metadata from Table object, prune and and return it as a dictionary"""
    d = tab.metadata.to_dict()

    # add columns
    # d["columns"] = {}
    # for col in tab.columns:
    #     d["columns"][col] = tab[col].metadata.to_dict()

    # sort primary key
    if "primary_key" in d:
        d["primary_key"] = sorted(d["primary_key"])

    del d["dataset"]
    return d


def _column_metadata_dict(meta: VariableMeta) -> Dict[str, Any]:
    d = meta.to_dict()
    d.pop("processing_log", None)
    return d


def _dataset_metadata_dict(ds: Dataset) -> Dict[str, Any]:
    """Extract metadata from Dataset object, prune and and return it as a dictionary"""
    d = ds.metadata.to_dict()

    # sort sources by name
    if "sources" in d:
        d["sources"] = sorted(d["sources"], key=lambda x: x.get("name") or "")

    d.pop("source_checksum", None)
    return d


def _local_catalog_datasets(
    catalog_path: Union[str, Path], channels: Iterable[CHANNEL], include: Optional[str], exclude: Optional[str]
) -> Dict[str, Dataset]:
    """Return a mapping from dataset path to Dataset object of local catalog."""
    catalog_path = Path(catalog_path)
    catalog_dir = catalog_path

    # it is possible to use subset of a data catalog
    while not (catalog_dir / "catalog.meta.json").exists() and catalog_dir != catalog_dir.parent:
        catalog_dir = catalog_dir.parent

    if catalog_dir != catalog_path:
        assert include is None, "Include pattern is not supported for subset of a catalog"
        include = str(catalog_path.relative_to(catalog_dir))

    lc_a = LocalCatalog(catalog_dir, channels=channels)
    datasets = []
    for chan in lc_a.channels:
        channel_datasets = list(lc_a.iter_datasets(chan, include=include))
        # TODO: channel should be in DatasetMeta by default
        for ds in channel_datasets:
            ds.metadata.channel = chan  # type: ignore

        datasets += channel_datasets

    # keep only relative path of dataset
    mapping = {str(Path(ds.path).relative_to(catalog_dir)): ds for ds in datasets}

    if exclude:
        re_exclude = re.compile(exclude)
        mapping = {path: ds for path, ds in mapping.items() if not re_exclude.search(path)}

    return mapping


def _fetch_remote_dataset(path: str, frame: pd.DataFrame) -> RemoteDataset:
    uri = f"{OWID_CATALOG_URI}{path}/index.json"
    js = requests.get(uri).json()
    # drop origins for backward compatibility
    js.pop("origins", None)
    ds_meta = DatasetMeta(**js)
    # TODO: channel should be in DatasetMeta by default
    ds_meta.channel = path.split("/")[0]  # type: ignore
    table_names = frame.loc[frame["ds_paths"] == path, "table"].tolist()
    return RemoteDataset(ds_meta, table_names)


def _remote_catalog_datasets(channels: Iterable[CHANNEL], include: str, exclude: Optional[str]) -> Dict[str, Dataset]:
    """Return a mapping from dataset path to Dataset object of remote catalog."""
    rc = RemoteCatalog(channels=channels)
    frame = rc.frame

    frame["ds_paths"] = frame["path"].map(os.path.dirname)

    # only compare public datasets
    frame = frame[frame.is_public]

    ds_paths = frame["ds_paths"]

    if include:
        ds_paths = ds_paths[ds_paths.str.contains(include, regex=True)]

    if exclude:
        ds_paths = ds_paths[~ds_paths.str.contains(exclude, regex=True)]

    ds_paths = set(ds_paths)

    if len(ds_paths) >= 10:
        log.warning(f"Fetching {len(ds_paths)} datasets from the remote catalog, this may get slow...")

    with ThreadPoolExecutor(max_workers=20) as executor:
        results = executor.map(
            lambda path: _fetch_remote_dataset(path, frame),
            ds_paths,
        )

    mapping = {path: result for path, result in zip(ds_paths, results)}

    return mapping  # type: ignore


def dataset_uri(ds: Dataset) -> str:
    # TODO: coule be method in DatasetMeta (after we add channel)
    assert hasattr(ds.metadata, "channel"), "Dataset metadata should have channel attribute"
    return f"{ds.metadata.channel}/{ds.metadata.namespace}/{ds.metadata.version}/{ds.metadata.short_name}"  # type: ignore


if __name__ == "__main__":
    cli()

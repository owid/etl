import difflib
import os
import re
import textwrap
import traceback
import urllib.error
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd
import requests
import rich
import rich_click as click
import structlog
from owid.catalog import Dataset, DatasetMeta, Table, VariableMeta, fetch
from owid.catalog.api.legacy import CHANNEL, ETLCatalog, LocalCatalog
from owid.catalog.api.utils import DEFAULT_CATALOG_URL
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from etl.dag_helpers import load_dag
from etl.data_helpers.misc import bard
from etl.datadiff_report import (
    SAMPLE_LIMIT,
    ColumnDiffResult,
    DatasetDiffResult,
    DiffReport,
    TableDiffResult,
    ValueDiff,
    format_score,
    render_html,
)
from etl.files import yaml_dump
from etl.git_helpers import get_changed_files
from etl.http import session as http_session
from etl.io import get_all_changed_catalog_paths
from etl.tempcompare import series_equals

log = structlog.get_logger()


class DatasetError(Exception):
    pass


class DatasetDiff:
    """Compare two datasets and print a summary of the differences."""

    def __init__(
        self,
        ds_a: Dataset | None,
        ds_b: Dataset | None,
        verbose: bool = False,
        cols: str | None = None,
        tables: str | None = None,
        print: Callable = rich.print,
        snippet: bool = False,
        country: str | None = None,
        details: bool = False,
    ):
        """
        :param cols: Only compare columns matching pattern
        :param tables: Only compare tables matching pattern
        :param print: Function to print the diff summary. Defaults to rich.print.
        :param snippet: Print snippet for loading both tables
        :param country: Filter tables by country if it is in the index
        :param details: Compute value-level diffs for the structured `result` even without verbose
        """
        assert ds_a or ds_b, "At least one Dataset must be provided"
        self.ds_a = ds_a
        self.ds_b = ds_b
        self.p = print
        self.verbose = verbose
        self.cols = cols
        self.tables = tables
        self.snippet = snippet
        self.country = country
        self.details = details
        # Structured mirror of the printed summary, filled in by summary().
        self.result = DatasetDiffResult(path=dataset_uri(ds_b or ds_a), kind="identical")  # type: ignore[arg-type]

    def _p_multiline(self, text: str) -> None:
        """Print a (possibly huge) multi-line diff body one line at a time.

        Rich's console rendering of a single giant `Text` blob scales badly with the number of
        embedded style spans: for WDI-sized metadata diffs (tens of thousands of lines, each
        wrapped in a color tag), a single `self.p(text)` call can take hours due to quadratic
        behavior in `Text.divide`/`Text.join` during soft-wrap layout. Printing line-by-line keeps
        each `Text` object small, which is linear instead (verified: 58k lines dropped from an
        unbounded hang to ~7s).
        """
        for line in text.split("\n"):
            self.p(line)

    def _filter_table_by_country(self, table: Table) -> Table:
        """Filter table by country if country is in the index."""
        if "country" in table.index.names:
            country_mask = table.index.get_level_values("country") == self.country
            return table.loc[country_mask].copy()
        elif "entity" in table.index.names:
            country_mask = table.index.get_level_values("entity") == self.country
            return table.loc[country_mask].copy()
        return table

    def _diff_datasets(self, ds_a: Dataset | None, ds_b: Dataset | None):
        if ds_a and ds_b:
            ds_short_name = ds_a.metadata.short_name
            assert ds_short_name

            new_version = " (new version)" if ds_a.metadata.version != ds_b.metadata.version else ""
            self.result.is_new_version = bool(new_version)

            # compare dataset metadata
            meta_a, meta_b = _dataset_metadata_dict(ds_a), _dataset_metadata_dict(ds_b)
            diff_lines = _diff_lines(meta_a, meta_b)
            diff = _format_diff(diff_lines, tabs=2)
            if diff:
                self.result.kind = "changed"
                self.result.meta_diff = _format_diff(diff_lines, tabs=0, color=False)
                self.p(f"[yellow]~ Dataset [b]{dataset_uri(ds_b)}[/b]{new_version}")
                if self.verbose:
                    self._p_multiline(diff)
            else:
                self.p(f"[white]= Dataset [b]{dataset_uri(ds_b)}{new_version}[/b]")
        elif ds_a:
            self.result.kind = "removed"
            self.p(f"[red]- Dataset [b]{dataset_uri(ds_a)}[/b]")
        elif ds_b:
            self.result.kind = "new"
            self.p(f"[green]+ Dataset [b]{dataset_uri(ds_b)}[/b]")
            for table_name in ds_b.table_names:
                self.p(f"\t[green]+ Table [b]{table_name}[/b]")
                tab_res = TableDiffResult(name=table_name, kind="new")
                self.result.tables.append(tab_res)
                for col in ds_b[table_name].columns:
                    self.p(f"\t\t[green]+ Column [b]{col}[/b]")
                    tab_res.columns.append(ColumnDiffResult(name=col, kind="new"))

    def _snippet(self, ds_a: Dataset, ds_b: Dataset, table_name: str) -> Panel:
        """Print code for loading both tables."""

        def _snippet_dataset(ds: Dataset, table_name: str) -> str:
            m = ds.metadata
            if isinstance(ds, RemoteDataset):
                return f'ETLCatalog(channels=["{m.channel}"]).find_one(table="{table_name}", dataset="{m.short_name}", version="{m.version}", namespace="{m.namespace}", channel="{m.channel}")'
            else:
                return f'Dataset(DATA_DIR / "{m.uri}")["{table_name}"]'

        code = f"""
from owid.catalog import ETLCatalog, Dataset
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
            tab_res = TableDiffResult(name=table_name, kind="removed")
            self.result.tables.append(tab_res)
            for col in ds_a[table_name].columns:
                self.p(f"\t\t[red]- Column [b]{col}[/b]")
                tab_res.columns.append(ColumnDiffResult(name=col, kind="removed"))
        elif table_name not in ds_a.table_names:
            self.p(f"\t[green]+ Table [b]{table_name}[/b]")
            tab_res = TableDiffResult(name=table_name, kind="new")
            self.result.tables.append(tab_res)
            for col in ds_b[table_name].columns:
                self.p(f"\t\t[green]+ Column [b]{col}[/b]")
                tab_res.columns.append(ColumnDiffResult(name=col, kind="new"))
        else:
            # get both tables in parallel
            with ThreadPoolExecutor() as executor:
                future_a = executor.submit(get_table_with_retry, ds_a, table_name)
                future_b = executor.submit(get_table_with_retry, ds_b, table_name)

                table_a = future_a.result()
                table_b = future_b.result()

            # set default index for datasets that don't have one
            if table_a.index.names == [None] and table_b.index.names == [None]:
                candidates = {"entity", "date", "country", "year"}
                new_index_cols = list(candidates & set(table_a.columns) & set(table_b.columns))
                if new_index_cols:
                    table_a = table_a.set_index(new_index_cols)
                    table_b = table_b.set_index(new_index_cols)

            # filter tables by country if specified and country is in the index
            if self.country:
                table_a = self._filter_table_by_country(table_a)
                table_b = self._filter_table_by_country(table_b)

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
            tab_meta_a, tab_meta_b = _table_metadata_dict(table_a), _table_metadata_dict(table_b)
            tab_diff_lines = _diff_lines(tab_meta_a, tab_meta_b)
            diff = _format_diff(tab_diff_lines, tabs=3)
            tab_res = TableDiffResult(name=table_name, kind="changed" if diff else "identical")
            self.result.tables.append(tab_res)
            if diff:
                tab_res.meta_diff = _format_diff(tab_diff_lines, tabs=0, color=False)
                self.p(f"\t[yellow]~ Table [b]{table_name}[/b] (changed [u]metadata[/u])")

                if self.verbose:
                    self._p_multiline(diff)
            else:
                self.p(f"\t[white]= Table [b]{table_name}[/b]")

            # compare index
            if not eq_index.all():
                for dim in dims:
                    if eq_index.all():
                        self.p(f"\t\t[white]= Dim [b]{dim}[/b]")
                    else:
                        self.p(f"\t\t[yellow]~ Dim [b]{dim}[/b]")
                        dim_res = ColumnDiffResult(name=dim, kind="changed", is_dim=True)
                        if new_index.any():
                            dim_res.changes.append("new data")
                        if removed_index.any():
                            dim_res.changes.append("removed data")
                        tab_res.columns.append(dim_res)
                        if self.verbose or self.details:
                            dims_without_dim = [d for d in dims if d != dim]
                            out, dim_res.value_diffs = _data_diff(
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
                            if self.verbose and out:
                                self._p_multiline(out)

            # compare columns
            all_cols = sorted((set(table_a.columns) | set(table_b.columns)) - set(dims))
            for col in all_cols:
                if self.cols and not re.search(self.cols, col):
                    continue

                if col not in table_a.columns:
                    self.p(f"\t\t[green]+ Column [b]{col}[/b]")
                    tab_res.columns.append(ColumnDiffResult(name=col, kind="new"))
                elif col not in table_b.columns:
                    self.p(f"\t\t[red]- Column [b]{col}[/b]")
                    tab_res.columns.append(ColumnDiffResult(name=col, kind="removed"))
                else:
                    col_a = table_a[col]
                    col_b = table_b[col]

                    # sort origins
                    # NOTE: they're excluded from _column_metadata_dict anyway
                    for tab in (table_a, table_b):
                        tab[col].m.origins = sorted(
                            tab[col].m.origins, key=lambda x: (x.title or "", x.title_snapshot or "")
                        )

                    # metadata diff
                    col_meta_a = _column_metadata_dict(col_a.metadata)
                    col_meta_b = _column_metadata_dict(col_b.metadata)
                    col_diff_lines = _diff_lines(col_meta_a, col_meta_b)
                    meta_diff = _format_diff(col_diff_lines, tabs=4)

                    # equality on index and series
                    eq_data = series_equals(table_a[col], table_b[col])

                    changed = []
                    if meta_diff:
                        changed.append("changed [u]metadata[/u]")
                    if new_index.any():
                        changed.append("new [u]data[/u]")
                    if (~eq_data[~new_index]).any():  # ty: ignore[call-non-callable]
                        changed.append("changed [u]data[/u]")

                    if changed:
                        col_res = ColumnDiffResult(
                            name=col,
                            kind="changed",
                            changes=[c.replace("[u]", "").replace("[/u]", "") for c in changed],
                        )
                        if meta_diff:
                            col_res.meta_diff = _format_diff(col_diff_lines, tabs=0, color=False)
                        tab_res.columns.append(col_res)
                        self.p(f"\t\t[yellow]~ Column [b]{col}[/b] ({', '.join(changed)})")
                        if self.verbose or self.details:
                            out = ""
                            if new_index.any() or removed_index.any() or (~eq_data).any():
                                out, col_res.value_diffs = _data_diff(
                                    table_a, table_b, col, dims, eq_data, eq_index, new_index, removed_index, tabs=4
                                )
                        if self.verbose:
                            if meta_diff:
                                self._p_multiline(meta_diff)
                            if new_index.any() or removed_index.any() or (~eq_data).any():
                                if meta_diff:
                                    self.p("")
                                if out:
                                    self._p_multiline(out)
                    else:
                        # do not print identical columns
                        pass

    def summary(self):
        """Print a summary of the differences between the two datasets."""
        self._diff_datasets(self.ds_a, self.ds_b)

        if self.ds_a and self.ds_b:
            for table_name in set(self.ds_a.table_names) | set(self.ds_b.table_names):
                if self.tables and not re.search(self.tables, table_name):
                    continue
                self._diff_tables(self.ds_a, self.ds_b, table_name)


class RemoteDataset:
    """Dataset from remote catalog with the same interface as Dataset."""

    def __init__(self, dataset_meta: DatasetMeta, table_names: list[str]):
        self.metadata = dataset_meta
        self.table_names = table_names

    def __getitem__(self, name: str) -> Table:
        tb_uri = f"{self.metadata.channel}/{self.metadata.namespace}/{self.metadata.version}/{self.metadata.short_name}/{name}"
        tb = fetch(tb_uri)
        return tb


def _dataset_files_match(ds_local: Dataset, ds_remote: "RemoteDataset") -> bool:
    """Return True if every local table's feather AND .meta.json file is byte-identical to its
    remote counterpart.

    Compares the local MD5 to the remote object's S3/R2 ETag (which is the file's MD5 for
    OWID's non-multipart uploads). Used to skip diffs when ``source_checksum`` cascades
    from an upstream change but neither the data nor the table metadata changed. Dataset-level
    metadata (``index.json``) is compared in-memory by the caller — its bytes always differ
    because they contain the source checksum itself.
    """
    from owid.catalog.core.datasets import checksum_file

    if set(ds_local.table_names) != set(ds_remote.table_names):
        return False

    local_md5: dict[str, str] = {}
    for t in ds_local.table_names:
        for fname in (f"{t}.feather", f"{t}.meta.json"):
            path = Path(ds_local.path) / fname
            if not path.exists():
                return False
            local_md5[fname] = checksum_file(path.as_posix()).hexdigest()

    base = (
        f"{DEFAULT_CATALOG_URL}{ds_remote.metadata.channel}/{ds_remote.metadata.namespace}/"
        f"{ds_remote.metadata.version}/{ds_remote.metadata.short_name}"
    )

    def _remote_md5(fname: str) -> str | None:
        try:
            r = http_session.head(f"{base}/{fname}", timeout=10)
        except requests.RequestException:
            return None
        if r.status_code != 200:
            return None
        return r.headers.get("ETag", "").strip('"').lower() or None

    with ThreadPoolExecutor(max_workers=min(8, max(1, len(local_md5)))) as executor:
        remote_md5 = dict(zip(local_md5.keys(), executor.map(_remote_md5, local_md5.keys())))

    return all(local_md5[f] == remote_md5.get(f) for f in local_md5)


def _changed_include_regex(include: str | None, catalog_paths: list[str]) -> str:
    """Build the --include regex for `--changed`, combining it with the user-supplied --include.

    Each catalog path is anchored at the end (but not the start — the local catalog matches
    against the full absolute directory path, e.g. ".../data/garden/.../wdi", not the bare
    "garden/.../wdi" the remote catalog uses). A bare substring join would let one changed
    dataset's short_name match as a *prefix* of another, unrelated dataset's short_name (e.g.
    "wb/2026-07-01/income_groups" matching inside ".../income_groups_aggregations"), sweeping it
    falsely into the comparison scope and reporting it as "removed" once it turns out not to be
    built locally either. Anchoring at the end rules that out while still matching both catalogs.
    """
    catalog_paths_pattern = "|".join(rf"{re.escape(p)}$" for p in catalog_paths)
    if include:
        # Positive look-aheads to match on both the user's --include and the changed paths.
        return rf"(?=.*{include})(?={catalog_paths_pattern})"
    return catalog_paths_pattern


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
    "--changed",
    is_flag=True,
    help="Only compare datasets with changes in git. This can significantly speed it up.",
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
    "--tables",
    type=str,
    help="Compare only tables matching pattern.",
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
    "--country",
    type=str,
    help="Filter tables by country if it is in the index.",
)
@click.option(
    "--workers",
    "-w",
    type=int,
    help="Use multiple threads.",
    default=1,
)
@click.option(
    "--output-json",
    type=click.Path(dir_okay=False),
    help="Write structured diff results as JSON to this path.",
)
@click.option(
    "--output-html",
    type=click.Path(dir_okay=False),
    help="Write a self-contained HTML report of the diff to this path.",
)
def cli(
    path_a: str,
    path_b: str,
    channel: Iterable[CHANNEL],
    changed: bool,
    include: str | None,
    cols: str | None,
    tables: str | None,
    exclude: str | None,
    verbose: bool,
    snippet: bool,
    country: str | None,
    workers: int,
    output_json: str | None,
    output_html: str | None,
) -> None:
    """Compare all datasets from two catalogs and print out a summary of their differences.

    Compare all the datasets from catalog in `PATH_A` with all the datasets in catalog `PATH_B`. The catalog paths link to the `data/` folder with all the datasets (it contains a `catalog.meta.json` file)

    You can also use a path to a dataset.

    Note that you can use the keyword "REMOTE" as the path, if you want to run a comparison with the remote catalog.

    This tool is useful as a quick way to see what has changed in the catalog and whether our updates don't have any unexpected side effects.

    **Note:** This command differs from `etl compare` in that it compares _all_ the datasets and not two specific ones.

    **How does it work?**

    It uses **source checksums** to find candidates for comparison. Source checksum includes all files used to generate the dataset and should be sufficient to find changed datasets, just note that we're not using checksum of the files themselves. So if you change core ETL code or some of the dependencies, e.g. change in owid-datautils-py, core ETL code or updating library version, the change won't be detected. In cases like these you should increment ETL version which is added to all source checksums (not implemented yet).

    **Example 1:** Compare the remote catalog with a local one for changed files

    ```
    $ etl diff REMOTE data/ --changed
    ```

    **Example 2:** Compare the remote catalog with a local one

    ```
    $ etl diff REMOTE data/ --include maddison
    ```

    **Example 3:** Compare two local catalogs

    ```
    $ etl diff other-data/ data/ --include maddison
    ```
    """
    console = Console(tab_size=2, soft_wrap=True)

    report = DiffReport()
    # collect value-level samples for the structured report even without --verbose
    details = bool(output_json or output_html)

    def _write_reports() -> None:
        if output_json:
            Path(output_json).write_text(report.to_json())
        if output_html:
            Path(output_html).write_text(render_html(report))

    if changed:
        # Get all changed files in the current git repository
        files_changed = get_changed_files()
        catalog_paths = get_all_changed_catalog_paths(files_changed)

        if not catalog_paths:
            _write_reports()
            console.print("[green]✅ No differences found[/green]")
            exit(0)

        include = _changed_include_regex(include, catalog_paths)

    path_to_ds_a = _load_catalog_datasets(path_a, channel, include, exclude)
    path_to_ds_b = _load_catalog_datasets(path_b, channel, include, exclude)

    # only keep datasets in DAG, unless there's only one dataset selected by precise path
    dag_steps = {s.split("://")[1] for s in load_dag().keys()}
    if len(path_to_ds_a) > 1:
        path_to_ds_a = {k: v for k, v in path_to_ds_a.items() if k in dag_steps}
    if len(path_to_ds_b) > 1:
        path_to_ds_b = {k: v for k, v in path_to_ds_b.items() if k in dag_steps}

    if not path_to_ds_a:
        _write_reports()
        console.print(f"[yellow]❓ No datasets found in {path_a}[/yellow]")
        exit(0)
    if not path_to_ds_b:
        _write_reports()
        console.print(f"[yellow]❓ No datasets found in {path_b}[/yellow]")
        exit(0)

    any_diff = False
    any_error = False

    matched_datasets = []
    skipped_identical_data = 0
    all_paths = sorted(set(path_to_ds_a.keys()) | set(path_to_ds_b.keys()))
    all_paths = [p for p in all_paths if not _is_superseded_remote_predecessor(p, path_to_ds_b)]
    for path in all_paths:
        ds_a = _match_dataset(path_to_ds_a, path)
        ds_b = _match_dataset(path_to_ds_b, path)

        if ds_a and ds_b and ds_a.metadata.source_checksum == ds_b.metadata.source_checksum:
            # skip if they have the same source checksum, note that we're not comparing checksum of actual data
            # to improve performance. Source checksum should be enough
            continue

        # Fast-path: source_checksum differs (typical when an upstream change cascades) but
        # nothing observable changed. Skip the full download/diff only when dataset-level
        # metadata is equal AND every table's feather + .meta.json is byte-identical (local MD5
        # vs remote S3/R2 ETag) — a metadata-only change must still get a full diff.
        if ds_a and ds_b:
            local_ds = ds_b if isinstance(ds_a, RemoteDataset) else ds_a
            remote_ds = ds_a if isinstance(ds_a, RemoteDataset) else (ds_b if isinstance(ds_b, RemoteDataset) else None)
            if (
                remote_ds is not None
                and isinstance(local_ds, Dataset)
                and _dataset_metadata_dict(ds_a) == _dataset_metadata_dict(ds_b)
                and _dataset_files_match(local_ds, remote_ds)
            ):
                skipped_identical_data += 1
                continue

        matched_datasets.append((ds_a, ds_b))

    if skipped_identical_data:
        report.skipped_cascade = skipped_identical_data
        log.info("Skipped datasets with identical data (source_checksum cascade)", count=skipped_identical_data)

    if workers > 1:
        futures = []

        with ThreadPoolExecutor(max_workers=workers) as executor:
            for ds_a, ds_b in matched_datasets:

                def func(ds_a, ds_b):
                    lines = []
                    differ = DatasetDiff(
                        ds_a,
                        ds_b,
                        cols=cols,
                        tables=tables,
                        print=lambda x: lines.append(x),
                        verbose=verbose,
                        snippet=snippet,
                        country=country,
                        details=details,
                    )
                    differ.summary()
                    return lines, differ.result

                futures.append(executor.submit(func, ds_a, ds_b))

            for (ds_a, ds_b), future in zip(matched_datasets, futures):
                try:
                    lines, result = future.result()
                except DatasetError as e:
                    # soft fail and continue with another dataset
                    lines = [f"[bold red]⚠ Error: {e}[/bold red]"]
                    result = DatasetDiffResult(path=dataset_uri(ds_b or ds_a), kind="error", error=str(e))
                except Exception as e:
                    # soft fail and continue with another dataset
                    log.error("\n".join(traceback.format_exception(type(e), e, e.__traceback__)))
                    any_error = True
                    report.datasets.append(
                        DatasetDiffResult(path=dataset_uri(ds_b or ds_a), kind="error", error=str(e))
                    )
                    lines = []
                    continue

                report.datasets.append(result)

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
                differ = DatasetDiff(
                    ds_a,
                    ds_b,
                    tables=tables,
                    cols=cols,
                    print=_append_and_print,
                    verbose=verbose,
                    snippet=snippet,
                    country=country,
                    details=details,
                )
                differ.summary()
            except DatasetError as e:
                # soft fail and continue with another dataset
                _append_and_print(f"[bold red]⚠ Error: {e}[/bold red]")
                report.datasets.append(DatasetDiffResult(path=dataset_uri(ds_b or ds_a), kind="error", error=str(e)))
                continue
            except Exception as e:
                # soft fail and continue with another dataset
                log.error("\n".join(traceback.format_exception(type(e), e, e.__traceback__)))
                any_error = True
                report.datasets.append(DatasetDiffResult(path=dataset_uri(ds_b or ds_a), kind="error", error=str(e)))
                continue

            report.datasets.append(differ.result)

            if any("~" in line for line in lines if isinstance(line, str)):
                any_diff = True

    _write_reports()

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


_DIFF_LINE_WRAP_WIDTH = 100


def _wrap_long_lines(text: str, width: int = _DIFF_LINE_WRAP_WIDTH) -> list[str]:
    """Split a YAML dump into diff-friendly lines, word-wrapping long ones.

    `yaml_dump`'s own `width` only wraps folded/plain scalars — literal block scalars (`|-`,
    used for any string containing no newlines of its own, e.g. `citation_full`) are emitted
    verbatim on one line however long. A single-word change at the end of an otherwise-identical
    few-hundred-character producer citation (a common shape: boilerplate text ending in
    "Accessed on <date>") then makes the *entire* line register as changed by the line-level diff
    below, duplicating the whole paragraph in both the removed and added sides. Wrapping first
    lets the line-level diff match all the unchanged wrapped chunks and only report the one that
    actually differs.
    """
    lines = []
    for line in text.splitlines(keepends=True):
        stripped = line.rstrip("\n")
        newline = line[len(stripped) :]
        indent = len(stripped) - len(stripped.lstrip(" "))
        if len(stripped) <= width:
            lines.append(line)
            continue
        wrapped = textwrap.wrap(
            stripped, width=width, subsequent_indent=" " * indent, break_long_words=False, break_on_hyphens=False
        )
        lines.extend(w + (newline if i == len(wrapped) - 1 else "\n") for i, w in enumerate(wrapped))
    return lines


def _diff_lines(dict_a: dict[str, Any], dict_b: dict[str, Any], **kwargs) -> list[str]:
    """Convert dictionaries into YAML and return the added/removed lines between them.

    Uses a plain line-level diff (`SequenceMatcher.get_opcodes`) rather than `difflib.ndiff`.
    `ndiff` additionally computes intraline ("? ") hints via its costly `_fancy_replace` pass, but
    every caller here immediately discards "? " and "  " (equal) lines, so that work is pure waste
    for wide, mostly-changed metadata like WDI's ~1500 columns.
    """
    meta_a = yaml_dump(dict_a, **kwargs)
    meta_b = yaml_dump(dict_b, **kwargs)

    a_lines = _wrap_long_lines(meta_a)
    b_lines = _wrap_long_lines(meta_b)

    lines = []
    for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(None, a_lines, b_lines).get_opcodes():
        if tag in ("delete", "replace"):
            lines.extend("- " + line for line in a_lines[i1:i2])
        if tag in ("insert", "replace"):
            lines.extend("+ " + line for line in b_lines[j1:j2])
    return lines


def _format_diff(lines: list[str], tabs: int = 0, color: bool = True) -> str:
    """Format pre-computed diff lines (from `_diff_lines`) as a colored/tabbed string."""
    if color:
        lines = ["[violet]" + line for line in lines]

    if not lines:
        return ""
    else:
        # add tabs
        return "\t" * tabs + "".join(lines).replace("\n", "\n" + "\t" * tabs).rstrip()


def _dict_diff(dict_a: dict[str, Any], dict_b: dict[str, Any], tabs: int = 0, color: bool = True, **kwargs) -> str:
    """Convert dictionaries into YAML and compare them using a line-level diff. Return colored diff as a string."""
    return _format_diff(_diff_lines(dict_a, dict_b, **kwargs), tabs=tabs, color=color)


def _df_to_str(df: pd.DataFrame, limit: int = 5) -> list[str]:
    lines = []
    if len(df) > limit:
        df_samp = df.sample(limit, random_state=0).sort_index()
    else:
        df_samp = df

    for line in df_samp.to_string(index=False).split("\n"):  # ty: ignore
        lines.append("  " + line)
    return lines


def _df_to_records(df: pd.DataFrame, limit: int = SAMPLE_LIMIT) -> list[dict[str, str]]:
    """Sample the dataframe and convert it to display-ready records with stringified values."""
    if len(df) > limit:
        df_samp = df.sample(limit, random_state=0).sort_index()
    else:
        df_samp = df

    def _fmt(v: Any) -> str:
        try:
            if pd.isna(v):
                return "NaN"
        except (TypeError, ValueError):
            pass
        return str(v)

    return [{str(k): _fmt(v) for k, v in row.items()} for row in df_samp.to_dict("records")]


def _changed_records(
    both: pd.DataFrame, col: str, limit: int = SAMPLE_LIMIT
) -> tuple[list[dict[str, str]], bool, float | None, int, int]:
    """Sample records for a changed-values diff.

    A value appearing or disappearing (NaN on one side) is a *coverage* event, not a value
    *revision* — e.g. the newest year in a new dataset version, or a slow-cadence indicator whose
    "latest" datapoint moves to a new year while the old one is dropped. Neither is an anomaly in
    the sense BARD measures (how much a value that existed on both sides moved), so those rows are
    excluded from the score entirely and counted separately as `appeared`/`disappeared` — reported
    on their own axis (`ColumnDiffResult.coverage_severity`) instead of competing with genuine
    revisions for the same score.

    For numeric columns, the sample keeps the most anomalous *revised* rows first (largest BARD —
    `etl.data_helpers.misc.bard`, the same metric Anomalist uses: bounded in [0, 1], symmetric,
    resistant to blow-ups on tiny values), with appeared/disappeared rows sorted after them and
    labeled as such instead of given a score. Other dtypes keep the plain random sample.

    Returns (records, sorted_by_score, median_bard, appeared_count, disappeared_count).
    median_bard is the median BARD across all *revised* rows (not just the sample) — the report
    uses it to sort columns, tables and datasets by how big their genuine revisions typically are.
    It's 0.0 (not None) when there are no revised rows, so a column that's all coverage churn
    scores as "no anomaly" rather than falling back to a non-numeric column's default of maximal.
    None only for non-numeric columns, where old/new can't be told apart from a category flip.
    """
    old_col, new_col = f"{col} -", f"{col} +"
    if not (pd.api.types.is_numeric_dtype(both[old_col]) and pd.api.types.is_numeric_dtype(both[new_col])):
        return _df_to_records(both, limit=limit), False, None, 0, 0

    old = both[old_col].astype("float64")
    new = both[new_col].astype("float64")
    old_isna = old.isna().to_numpy()
    new_isna = new.isna().to_numpy()
    appeared = old_isna & ~new_isna
    disappeared = ~old_isna & new_isna
    revised = ~old_isna & ~new_isna

    score = np.full(len(both), np.nan)
    score[revised] = bard(old.to_numpy()[revised], new.to_numpy()[revised])
    median_bard = float(np.median(score[revised])) if revised.any() else 0.0

    # Revised rows rank by score (largest first); appeared/disappeared always sort after them —
    # they're not ranked anomalies — with ties broken by original order (stable sort).
    rank_key = np.where(revised, -score, np.inf)
    order = np.argsort(rank_key, kind="stable")[:limit]
    top = both.iloc[order].copy()
    top["anomaly score"] = [
        format_score(score[i]) if revised[i] else ("appeared" if appeared[i] else "disappeared") for i in order
    ]
    return _df_to_records(top, limit=limit), True, median_bard, int(appeared.sum()), int(disappeared.sum())


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
) -> tuple[str, list[ValueDiff]]:
    """Return summary of data differences as text and as structured value diffs."""
    # eq = eq_data & eq_index
    n = (eq_index | new_index).sum()

    lines = []
    value_diffs = []

    cols = [d for d in dims if d is not None] + [col]

    # new values
    if new_index.any():
        lines.append(
            f"+ New values: {new_index.sum()} / {n} ({new_index.sum() / n * 100:.2f}%)",
        )
        new_values = table_b.loc[new_index, cols]
        lines += _df_to_str(new_values)
        value_diffs.append(
            ValueDiff(kind="new", count=int(new_index.sum()), total=int(n), sample=_df_to_records(new_values))
        )

    # removed values
    if removed_index.any():
        lines.append(
            f"- Removed values: {removed_index.sum()} / {n} ({removed_index.sum() / n * 100:.2f}%)",
        )
        removed_values = table_a.loc[removed_index, cols]
        lines += _df_to_str(removed_values)
        value_diffs.append(
            ValueDiff(
                kind="removed", count=int(removed_index.sum()), total=int(n), sample=_df_to_records(removed_values)
            )
        )

    # changed values
    neq = ~eq_data & eq_index
    if neq.any():
        lines.append(
            f"~ Changed values: {neq.sum()} / {n} ({neq.sum() / n * 100:.2f}%)",
        )
        # Merge as plain pandas DataFrames, not Table.merge/join. The OWID Table variants also
        # combine per-column metadata (origins/sources) into the result, which is expensive for
        # wide, heavily-sourced datasets like WDI — and wasted here since `both` is only used for
        # its raw values (sampling/scoring below), never its metadata.
        samp_a = pd.DataFrame(table_a.loc[neq, cols])
        samp_b = pd.DataFrame(table_b.loc[neq, cols])
        if dims:
            both = samp_a.merge(samp_b, on=dims, suffixes=(" -", " +"))
        else:
            both = samp_a.join(samp_b, lsuffix=" -", rsuffix=" +")
        lines += _df_to_str(both)

        records, sorted_by_score, median_bard, appeared_count, disappeared_count = _changed_records(both, col)
        value_diffs.append(
            ValueDiff(
                kind="changed",
                count=int(neq.sum()),
                total=int(n),
                sample=records,
                sorted_by_score=sorted_by_score,
                median_bard=median_bard,
                appeared_count=appeared_count,
                disappeared_count=disappeared_count,
            )
        )

    # add color
    lines = ["[violet]" + line for line in lines]

    if not lines:
        return "", value_diffs
    else:
        # add tabs
        return "\t" * tabs + "\n".join(lines).replace("\n", "\n" + "\t" * tabs).rstrip(), value_diffs

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
        return np.issubdtype(dtype, np.datetime64)  # ty: ignore
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
            level = level.reorder_categories(sorted(level.categories))  # ty: ignore[unresolved-attribute]
        new_levels.append(level)

    df.index = pd.MultiIndex.from_arrays(new_levels)
    df.sort_index(inplace=True)
    return df


def _match_dataset(path_to_ds: dict[str, Any], path: str) -> Dataset | None:
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


def _is_superseded_remote_predecessor(path: str, path_to_ds_b: dict[str, Any]) -> bool:
    """True if `path` is only in scope as a REMOTE-side fallback match for a newer local version.

    `--changed` adds a changed dataset's predecessor version to the shared `--include` filter so
    the REMOTE fetch includes it — `_match_dataset` then falls back to it when diffing the new
    local version against something. But the union-of-keys loop in `cli()` also visits the
    predecessor's own path directly; if that exact version isn't *also* built locally (the normal
    case — only the new version was run), it compares against nothing and gets reported as a
    separate, false "removed dataset", right back to the bug this scoping exists to avoid. Skip a
    path here when it's absent locally but a newer version of the same
    channel/namespace/short_name is present — `_match_dataset` already reaches it as a fallback
    for that newer path, so it needs no standalone entry of its own.
    """
    if path in path_to_ds_b:
        return False
    channel, namespace, version, short_name = path.split("/")
    pattern = re.compile(rf"^{re.escape(channel)}/{re.escape(namespace)}/([^/]+)/{re.escape(short_name)}$")
    return any((match := pattern.match(k)) and match.group(1) > version for k in path_to_ds_b)


def _load_catalog_datasets(
    catalog_path: str, channels: Iterable[CHANNEL], include: str | None, exclude: str | None
) -> dict[str, Any]:
    if catalog_path == "REMOTE":
        assert include, "You have to filter with --include when comparing with remote catalog"
        return _remote_catalog_datasets(channels=channels, include=include, exclude=exclude)
    else:
        return _local_catalog_datasets(catalog_path, channels=channels, include=include, exclude=exclude)


def _table_metadata_dict(tab: Table) -> dict[str, Any]:
    """Extract metadata from Table object, prune and and return it as a dictionary"""
    d = tab.metadata.to_dict()

    # collect unique origins from all columns
    origins = set()
    for col in tab.columns:
        origins.update(tab[col].metadata.origins)
    # Sort by enough fields to be a total order over real-world origins, not just a tie-breaker.
    # For datasets like WDI, every origin shares the same title/title_snapshot (one indicator per
    # column, but all indicators come from the same producer, description and citation
    # boilerplate), so those two fields alone leave the sort a no-op tied to `set()`'s arbitrary
    # iteration order. Two independently-built tables (old vs. new version) then serialize their
    # ~1500 origins in unrelated orders, and the line-level diff below sees the whole list as
    # reshuffled — every origin (unchanged fields included) shows as removed+added rather than
    # only the handful of lines that actually changed. `url_main` disambiguates down to the
    # individual indicator, making the order (and hence the diff) stable and content-based.
    d["origins"] = sorted(
        origins, key=lambda x: (x.title or "", x.title_snapshot or "", x.producer or "", x.url_main or "")
    )

    # add columns
    # d["columns"] = {}
    # for col in tab.columns:
    #     d["columns"][col] = tab[col].metadata.to_dict()

    # sort primary key
    if "primary_key" in d:
        d["primary_key"] = sorted(d["primary_key"])

    del d["dataset"]
    return d


def _column_metadata_dict(meta: VariableMeta) -> dict[str, Any]:
    d = meta.to_dict()

    # remove origins, they're displayed on table level
    d.pop("origins", None)

    # remove noise

    for source in d.get("sources", []):
        source.pop("date_accessed", None)
        source.pop("publication_date", None)
    return d


def _dataset_metadata_dict(ds: Dataset) -> dict[str, Any]:
    """Extract metadata from Dataset object, prune and and return it as a dictionary"""
    d = ds.metadata.to_dict()

    # sort sources by name
    if "sources" in d:
        d["sources"] = sorted(d["sources"], key=lambda x: x.get("name") or "")

    d.pop("source_checksum", None)
    return d


def _local_catalog_datasets(
    catalog_path: str | Path, channels: Iterable[CHANNEL], include: str | None, exclude: str | None
) -> dict[str, Dataset]:
    """Return a mapping from dataset path to Dataset object of local catalog."""
    catalog_path = Path(catalog_path)
    catalog_dir = catalog_path

    # it is possible to use subset of a data catalog
    if catalog_dir.name != "data":
        while catalog_dir != catalog_dir.parent:
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
            ds.metadata.channel = chan  # ty: ignore

        datasets += channel_datasets

    # only compare public datasets
    datasets = [ds for ds in datasets if ds.is_public]

    # keep only relative path of dataset
    mapping = {str(Path(ds.path).relative_to(catalog_dir)): ds for ds in datasets}

    if exclude:
        re_exclude = re.compile(exclude)
        mapping = {path: ds for path, ds in mapping.items() if not re_exclude.search(path)}

    return mapping


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.RequestException),
)
def _fetch_remote_dataset(path: str, frame: pd.DataFrame) -> RemoteDataset:
    uri = f"{DEFAULT_CATALOG_URL}{path}/index.json"
    js = http_session.get(uri, timeout=30).json()
    # Drop deprecated provenance fields for backward compatibility with remote catalog entries
    # that were built before the Source -> Origin migration was fully rolled out.
    js.pop("origins", None)
    js.pop("sources", None)
    ds_meta = DatasetMeta(**js)
    # TODO: channel should be in DatasetMeta by default
    ds_meta.channel = path.split("/")[0]  # ty: ignore
    table_names = frame.loc[frame["ds_paths"] == path, "table"].tolist()
    return RemoteDataset(ds_meta, table_names)


def _remote_catalog_datasets(channels: Iterable[CHANNEL], include: str, exclude: str | None) -> dict[str, Dataset]:
    """Return a mapping from dataset path to Dataset object of remote catalog."""
    rc = ETLCatalog(channels=channels)
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

    return mapping  # ty: ignore


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(urllib.error.HTTPError),
)
def get_table_with_retry(ds: Dataset, table_name: str) -> Table:
    return ds[table_name]


def dataset_uri(ds: Dataset) -> str:
    # TODO: coule be method in DatasetMeta (after we add channel)
    assert hasattr(ds.metadata, "channel"), "Dataset metadata should have channel attribute"
    return f"{ds.metadata.channel}/{ds.metadata.namespace}/{ds.metadata.version}/{ds.metadata.short_name}"  # ty: ignore


if __name__ == "__main__":
    cli()

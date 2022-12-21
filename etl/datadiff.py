import os
import re
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

import requests
import rich
import rich_click as click
import structlog
from deepdiff import DeepDiff
from owid.catalog import Dataset, DatasetMeta, LocalCatalog, RemoteCatalog, Table, find
from owid.catalog.catalogs import CHANNEL, OWID_CATALOG_URI
from rich.console import Console

log = structlog.get_logger()


class DatasetDiff:
    def __init__(self, ds_a: Optional[Dataset], ds_b: Optional[Dataset], print: Callable = rich.print):
        assert ds_a or ds_b, "At least one Dataset must be provided"
        self.ds_a = ds_a
        self.ds_b = ds_b
        self.p = print

    def _diff_datasets(self, ds_a: Optional[Dataset], ds_b: Optional[Dataset]):
        if ds_a and ds_b:
            ds_short_name = ds_a.metadata.short_name
            assert ds_short_name

            # compare dataset metadata
            diff = DeepDiff(_dataset_metadata_dict(ds_a), _dataset_metadata_dict(ds_b))
            if diff:
                self.p(f"[yellow]~ Dataset [b]{dataset_uri(ds_a)}[/b]")
                # self.p(diff)
            else:
                self.p(f"[white]= Dataset [b]{dataset_uri(ds_a)}[/b]")
        elif ds_a:
            self.p(f"[red]- Dataset [b]{dataset_uri(ds_a)}[/b]")
        elif ds_b:
            self.p(f"[green]+ Dataset [b]{dataset_uri(ds_b)}[/b]")

    def _diff_tables(self, ds_a: Dataset, ds_b: Dataset, table_name: str):
        if table_name not in ds_b.table_names:
            self.p(f"\t[red]- Table [b]{table_name}[/b]")
            for col in ds_a[table_name].columns:
                self.p(f"\t\t[red]- Column [b]{col}[/b]")
        elif table_name not in ds_a.table_names:
            self.p(f"\t[green]+ Table [b]{table_name}[/b]")
            for col in ds_b[table_name].columns:
                self.p(f"\t\t[green]+ Column [b]{col}[/b]")
        else:
            table_a = ds_a[table_name].reset_index()
            table_b = ds_b[table_name].reset_index()

            # compare table metadata
            diff = DeepDiff(_table_metadata_dict(table_a), _table_metadata_dict(table_b))
            if diff:
                self.p(f"\t[yellow]~ Table [b]{table_name}[/b]")
                # self.p(diff)
            else:
                self.p(f"\t[white]= Table [b]{table_name}[/b]")

            # compare columns
            for col in sorted(set(table_a.columns) | set(table_b.columns)):
                if col not in table_a.columns:
                    self.p(f"\t\t[green]+ Column [b]{col}[/b]")
                elif col not in table_b.columns:
                    self.p(f"\t\t[red]- Column [b]{col}[/b]")
                else:
                    col_a = table_a[col]
                    col_b = table_b[col]
                    shape_diff = col_a.shape != col_b.shape
                    if not shape_diff:
                        data_diff = not col_a.equals(col_b)
                    else:
                        data_diff = False
                    meta_diff = DeepDiff(col_a.metadata.to_dict(), col_b.metadata.to_dict())

                    changed = (
                        (["data"] if data_diff else [])
                        + (["metadata"] if meta_diff else [])
                        + (["shape"] if shape_diff else [])
                    )

                    if changed:
                        self.p(f"\t\t[yellow]~ Column [b]{col}[/b] (changed [u]{' & '.join(changed)}[/u])")
                    else:
                        # do not print identical columns
                        pass

    def summary(self):
        self._diff_datasets(self.ds_a, self.ds_b)

        if self.ds_a and self.ds_b:
            for table_name in set(self.ds_a.table_names) | set(self.ds_b.table_names):
                self._diff_tables(self.ds_a, self.ds_b, table_name)


@click.command(help=__doc__)
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
    help="Compare only selected channel (subfolder of data/), compare only meadow, garden and grapher by default",
)
@click.option(
    "--include",
    type=str,
    help="Compare only datasets matching pattern",
)
@click.option(
    "--exclude",
    "-e",
    type=str,
    help="Exclude datasets matching pattern",
)
def cli(
    path_a: str,
    path_b: str,
    channel: Iterable[CHANNEL],
    include: Optional[str],
    exclude: Optional[str],
) -> None:
    """TODO

    Usage:
        etl-datadiff data/ other/ -c garden --include maddison
    """
    console = Console(tab_size=2)

    console.print(
        "[b]Legend[/b]: [green]+New[/green]  [yellow]~Modified[/yellow]  [red]-Removed[/red]  [white]=Identical[/white]\n"
    )

    path_to_ds_a = _local_catalog_datasets(path_a, channels=channel, include=include, exclude=exclude)

    if path_b == "REMOTE":
        assert include, "You have to filter with --include when comparing with remote catalog"
        path_to_ds_b = _remote_catalog_datasets(channels=channel, include=include, exclude=exclude)
    else:
        path_to_ds_b = _local_catalog_datasets(path_b, channels=channel, include=include, exclude=exclude)

    any_diff = False

    for path in set(path_to_ds_a.keys()) | set(path_to_ds_b.keys()):
        ds_a = path_to_ds_a.get(path)
        ds_b = path_to_ds_b.get(path)

        if ds_a and ds_b and ds_a.metadata.source_checksum == ds_b.metadata.source_checksum:
            # skip if they have the same source checksum, note that we're not comparing checksum of actual data
            # to improve performance. Source checksum should be enough
            continue

        lines = []
        differ = DatasetDiff(ds_a, ds_b, print=lambda x: lines.append(x))
        differ.summary()

        for line in lines:
            console.print(line)

        if any("~" in line for line in lines):
            any_diff = True

    exit(1 if any_diff else 0)


def _table_metadata_dict(tab: Table) -> Dict[str, Any]:
    d = tab.metadata.to_dict()

    # add columns
    d["columns"] = {}
    for col in tab.columns:
        d["columns"][col] = tab[col].metadata.to_dict()

    del d["dataset"]
    return d


def _dataset_metadata_dict(ds: Dataset) -> Dict[str, Any]:
    d = ds.metadata.to_dict()
    del d["source_checksum"]
    return d


def _local_catalog_datasets(
    catalog_path: str, channels: Iterable[CHANNEL], include: Optional[str], exclude: Optional[str]
) -> Dict[str, Dataset]:
    """Return a mapping from dataset path to Dataset object"""
    lc_a = LocalCatalog(catalog_path, channels=channels)
    datasets = []
    for chan in lc_a.channels:
        datasets += list(lc_a.iter_datasets(chan, include=include))
        # TODO: channel should be in DatasetMeta by default
        for ds in datasets:
            ds.metadata.channel = chan

    # keep only relative path of dataset
    mapping = {str(Path(ds.path).relative_to(catalog_path)): ds for ds in datasets}

    if exclude:
        re_exclude = re.compile(exclude)
        mapping = {path: ds for path, ds in mapping.items() if not re_exclude.search(path)}

    return mapping


class RemoteDataset:
    """Dataset from remote catalog with the same interface as Dataset."""

    def __init__(self, dataset_meta: DatasetMeta, table_names: List[str]):
        self.metadata = dataset_meta
        self.table_names = table_names

    def __getitem__(self, name: str) -> Table:
        tables = find(
            table=name,
            namespace=self.metadata.namespace,
            version=self.metadata.version,
            dataset=self.metadata.short_name,
            channels=[self.metadata.channel],  # type: ignore
        )

        tables = tables[tables.channel == self.metadata.channel]  # type: ignore

        return tables.load()


def _remote_catalog_datasets(channels: Iterable[CHANNEL], include: str, exclude: Optional[str]) -> Dict[str, Dataset]:
    """Return a mapping from dataset path to Dataset object"""
    rc = RemoteCatalog(channels=channels)
    frame = rc.frame

    frame["ds_paths"] = frame["path"].map(os.path.dirname)
    ds_paths = frame["ds_paths"]

    if include:
        ds_paths = ds_paths[ds_paths.str.contains(include)]

    if exclude:
        ds_paths = ds_paths[~ds_paths.str.contains(exclude)]

    ds_paths = set(ds_paths)

    if len(ds_paths) >= 10:
        log.warning(f"Fetching {len(ds_paths)} datasets from the remote catalog, this may get slow...")

    mapping = {}
    for path in ds_paths:
        uri = f"{OWID_CATALOG_URI}{path}/index.json"
        ds_meta = DatasetMeta(**requests.get(uri).json())
        # TODO: channel should be in DatasetMeta by default
        ds_meta.channel = path.split("/")[0]  # type: ignore
        table_names = frame.loc[frame["ds_paths"] == path, "table"].tolist()
        mapping[path] = RemoteDataset(ds_meta, table_names)

    return mapping


def dataset_uri(ds: Dataset) -> str:
    assert hasattr(ds.metadata, "channel"), "Dataset metadata should have channel attribute"
    return f"{ds.metadata.channel}/{ds.metadata.namespace}/{ds.metadata.version}/{ds.metadata.short_name}"  # type: ignore


if __name__ == "__main__":
    cli()

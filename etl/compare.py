#
#  compare.py
#

from pathlib import Path
import click
from rich_click.rich_command import RichCommand
from rich_click.rich_group import RichGroup
from rich import print
from typing import cast
import pandas as pd
from etl import tempcompare
from owid import catalog


@click.group(cls=RichGroup)
def cli() -> None:
    """Compare two dataframes, both structurally and the values.

    This tool loads two dataframes, either from the local ETL and the remote catalog
    or just from two different files. It compares the columns, index columns and index values (row indices) as
    sets between the two dataframes and outputs the differences. Finally it compares the values of the overlapping
    columns and rows with the given threshold values for absolute and relative tolerance.

    The exit code is 0 if the dataframes are equal, 1 if there is an error loading the dataframes, 2 if the dataframes
    are structurally equal but are otherwise different, 3 if the dataframes have different structure and/or different values.
    """
    pass


def diff_print_and_exit(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    df1_label: str,
    df2_label: str,
    absolute_tolerance: float,
    relative_tolerance: float,
    show_values: bool,
    show_shared: bool,
    truncate_lists_at: int,
) -> None:
    """Runs the comparison and prints the differences, then exits with the appropriate exit code."""
    diff = tempcompare.HighLevelDiff(df1, df2, absolute_tolerance, relative_tolerance)
    if diff.are_equal:
        print("[green]Dataframes are equal (within the given thresholds)[/green]")
        exit(0)
    else:
        lines = diff.get_description_lines_for_diff(
            df1_label,
            df2_label,
            use_color_tags=True,
            preview_different_dataframe_values=show_values,
            show_shared=show_shared,
            truncate_lists_longer_than=truncate_lists_at,
        )
        for line in lines:
            print(line)
        if diff.are_structurally_equal:
            exit(2)
        else:
            exit(3)


@cli.command(cls=RichCommand)
@click.argument("channel")
@click.argument("namespace")
@click.argument("dataset")
@click.argument("table")
@click.option(
    "--absolute-tolerance",
    default=0.00000001,
    show_default=True,
    help="The absolute tolerance for floating point comparisons.",
)
@click.option(
    "--relative-tolerance",
    default=0.05,
    show_default=True,
    help="The relative tolerance for floating point comparisons.",
)
@click.option(
    "--show-values/--hide-values",
    default=False,
    show_default=True,
    help="Show a preview of the values where the dataframes are different.",
)
@click.option(
    "--show-shared/--hide-shared",
    default=False,
    show_default=True,
    help="Show the structural overlap of the two dataframes (shared columns, index columns and index values).",
)
@click.option(
    "--truncate-lists-at",
    default=20,
    show_default=True,
    help="Print truncated lists if they are longer than the given length.",
)
def etl_catalog(
    channel: str,
    namespace: str,
    dataset: str,
    table: str,
    absolute_tolerance: float,
    relative_tolerance: float,
    show_values: bool,
    show_shared: bool,
    truncate_lists_at: int,
) -> None:
    """
    Compare a table in the local catalog with the one in the remote catalog.

    It compares the columns, index columns and index values (row indices) as
    sets between the two dataframes and outputs the differences. Finally it compares the values of the overlapping
    columns and rows with the given threshold values for absolute and relative tolerance.

    The exit code is 0 if the dataframes are equal, 1 if there is an error loading the dataframes, 2 if the dataframes
    are structurally equal but are otherwise different, 3 if the dataframes have different structure and/or different values.
    """
    try:
        remote_df = catalog.find_one(
            table=table, namespace=namespace, dataset=dataset, channels=channel
        )
        local_catalog = catalog.LocalCatalog("data")
        local_df = local_catalog.find_one(
            table=table,
            namespace=namespace,
            dataset=dataset,
            channel=cast(catalog.CHANNEL, channel),
        )
    except Exception as e:
        print(e)
        exit(1)

    diff_print_and_exit(
        remote_df,
        local_df,
        "remote",
        "local",
        absolute_tolerance,
        relative_tolerance,
        show_values,
        show_shared,
        truncate_lists_at,
    )


def load_table(path_str: str) -> catalog.Table:
    """Loads a Table (dataframe + metadata) from a path."""
    path = Path(path_str)
    if not path.exists():
        raise Exception("File does not exist: " + path_str)

    if path.suffix.lower() == ".feather":
        return catalog.tables.Table.read_feather(path_str)
    elif path.suffix.lower() == ".csv":
        return catalog.tables.Table.read_csv(path_str)
    else:
        raise Exception("Unknown file format: " + path_str)


def load_dataframe(path_str: str) -> pd.DataFrame:
    """Loads a DataFrame from a path."""
    path = Path(path_str)
    if not path.exists():
        raise Exception("File does not exist: " + path_str)

    if path.suffix.lower() == ".feather":
        return cast(pd.DataFrame, pd.read_feather(path_str))
    elif path.suffix.lower() == ".csv":
        return pd.read_csv(path_str)
    elif path.suffix.lower() == ".parquet":
        return cast(pd.DataFrame, pd.read_parquet(path_str))
    else:
        raise Exception("Unknown file format: " + path_str)


@cli.command(cls=RichCommand)
@click.argument("dataframe1")
@click.argument("dataframe2")
@click.option(
    "--absolute-tolerance",
    default=0.00000001,
    show_default=True,
    help="The absolute tolerance for floating point comparisons.",
)
@click.option(
    "--relative-tolerance",
    default=0.05,
    show_default=True,
    help="The relative tolerance for floating point comparisons.",
)
@click.option(
    "--show-values/--hide-values",
    default=False,
    show_default=True,
    help="Show a preview of the values where the dataframes are different.",
)
@click.option(
    "--show-shared/--hide-shared",
    default=False,
    show_default=True,
    help="Show the structural overlap of the two dataframes (shared columns, index columns and index values).",
)
@click.option(
    "--truncate-lists-at",
    default=20,
    show_default=True,
    help="Print truncated lists if they are longer than the given length.",
)
def dataframes(
    dataframe1: str,
    dataframe2: str,
    absolute_tolerance: float,
    relative_tolerance: float,
    show_values: bool,
    show_shared: bool,
    truncate_lists_at: int,
) -> None:
    """
    Compare two dataframes given as paths.

    It compares the columns, index columns and index values (row indices) as
    sets between the two dataframes and outputs the differences. Finally it compares the values of the overlapping
    columns and rows with the given threshold values for absolute and relative tolerance.

    The exit code is 0 if the dataframes are equal, 1 if there is an error loading the dataframes, 2 if the dataframes
    are structurally equal but are otherwise different, 3 if the dataframes have different structure and/or different values.
    """
    df1: pd.DataFrame
    df2: pd.DataFrame
    print("🦸 OWID's friendly dataframe comparision tool - at your service! 🦸")
    try:
        df1 = load_table(dataframe1)
    except Exception:
        print(
            f"[yellow]Reading {dataframe1} as table with metadata failed, trying to read as plain dataframe[/yellow]"
        )
        try:
            df1 = load_dataframe(dataframe1)
        except Exception as e:
            print(f"[red]Reading {dataframe1} as dataframe failed[/red]")
            print(e)
            exit(1)

    try:
        df2 = load_table(dataframe2)
    except Exception:
        print(
            f"[yellow]Reading {dataframe2} as table with metadata failed, trying to read as plain dataframe[/yellow]"
        )
        try:
            df2 = load_dataframe(dataframe2)
        except Exception as e:
            print(f"[red]Reading {dataframe2} as dataframe failed[/red]")
            print(e)
            exit(1)

    diff_print_and_exit(
        df1,
        df2,
        "dataframe1",
        "dataframe2",
        absolute_tolerance,
        relative_tolerance,
        show_values,
        show_shared,
        truncate_lists_at,
    )


if __name__ == "__main__":
    cli()

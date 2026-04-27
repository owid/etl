"""Format DAG YAML files — compact linear chains into nested syntax or flatten back.

The DAG YAML format supports two equivalent syntaxes for step dependencies:

* **Flat** (historical) — every step is a top-level key under ``steps:``::

    data://meadow/un/2022-07-11/un_wpp:
      - snapshot://un/2022-07-11/un_wpp.zip
    data://garden/un/2022-07-11/un_wpp:
      - data://meadow/un/2022-07-11/un_wpp
    data://grapher/un/2022-07-11/un_wpp:
      - data://garden/un/2022-07-11/un_wpp

* **Nested** (compact) — linear chains collapse into indented list items::

    data://grapher/un/2022-07-11/un_wpp:
      - data://garden/un/2022-07-11/un_wpp:
        - data://meadow/un/2022-07-11/un_wpp:
          - snapshot://un/2022-07-11/un_wpp.zip

Use ``etl dag compact`` to rewrite active DAG files into the nested form where
possible. A step ``D`` is folded into its parent ``P`` iff ``D`` has a single
global consumer (``P``) — shared dependencies stay flat so each step is still
declared exactly once. Use ``etl dag flatten`` for the inverse (mostly useful
for reviewing diffs or investigating a chain).
"""

from pathlib import Path

import rich_click as click

from etl import paths
from etl.dag_helpers import build_consumer_graph, compact_dag_file, flatten_dag_file


def _expand_files(files: tuple[str, ...]) -> list[Path]:
    if files:
        return [Path(f) for f in files]
    return sorted(p for p in Path(paths.DAG_DIR).glob("*.yml"))


@click.group(name="dag", context_settings=dict(show_default=True))
def cli() -> None:
    """Format DAG YAML files between flat and nested syntax."""


@cli.command(name="compact")
@click.argument("files", nargs=-1, type=click.Path(exists=True, path_type=Path))
def compact_cmd(files: tuple[str, ...]) -> None:
    """Fold linear chains into the nested syntax for each FILE.

    Defaults to every ``dag/*.yml`` file when no FILE is given (archived DAG
    files under ``dag/archive/`` are skipped). Cross-file consumers are
    respected — a step shared by steps in another DAG file is kept flat.
    """
    targets = _expand_files(files)
    if not targets:
        click.echo("No DAG files found.")
        return

    # Build the consumer graph once and share it — loading the full DAG for
    # every file would be wasteful.
    consumers = build_consumer_graph()

    changed = 0
    for target in targets:
        if compact_dag_file(target, consumers=consumers):
            click.echo(f"compacted {target}")
            changed += 1
    click.echo(f"{changed}/{len(targets)} files changed")


@cli.command(name="flatten")
@click.argument("files", nargs=-1, type=click.Path(exists=True, path_type=Path))
def flatten_cmd(files: tuple[str, ...]) -> None:
    """Expand nested dep syntax back to the flat ``step: [deps]`` form.

    Defaults to every ``dag/*.yml`` file when no FILE is given.
    """
    targets = _expand_files(files)
    if not targets:
        click.echo("No DAG files found.")
        return

    changed = 0
    for target in targets:
        if flatten_dag_file(target):
            click.echo(f"flattened {target}")
            changed += 1
    click.echo(f"{changed}/{len(targets)} files changed")

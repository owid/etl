"""Minimal ETL logging with rich formatting."""

from rich.console import Console

_console = Console(highlight=False)


def snapshot(msg: str) -> None:
    _console.print(f"  [dim]📦 {msg}[/]")


def dataset(msg: str) -> None:
    _console.print(f"  [bold]📊 {msg}[/]")


def action(msg: str) -> None:
    _console.print(f"  [bold]⚡ {msg}[/]")


def skip(name: str) -> None:
    _console.print(f"  [dim]· {name} up to date[/]")


def step(name: str) -> None:
    _console.print(f"[bold blue]→ {name}[/]")

"""Nice console UI helpers."""

import sys
from typing import NoReturn

import click


def log(action: str, message: str) -> None:
    """Log message."""
    action = f"{action:20s}"
    click.echo(f"{blue(action)}{message}")


def bail(message: str) -> NoReturn:
    """Print an error message then exit."""
    print(f'{red("ERROR:")} {message}', file=sys.stderr)
    sys.exit(1)


def blue(s: str) -> str:
    """Add click style (blue color)."""
    return _colorize(s, fg="blue")


def red(s: str) -> str:
    """Add click style (red color)."""
    return _colorize(s, fg="red")


def _colorize(s: str, fg: str) -> str:
    return click.style(s, fg=fg)

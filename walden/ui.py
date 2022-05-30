#
#  ui.py
#
#  Nice console UI helpers.
#

import sys
from typing import NoReturn

import click


def blue(s: str) -> str:
    return click.style(s, fg="blue")


def log(action: str, message: str) -> None:
    action = f"{action:20s}"
    click.echo(f"{blue(action)}{message}")


def red(s: str) -> str:
    return click.style(s, fg="red")


def bail(message: str) -> NoReturn:
    "Print an error message then exit."
    print(f'{red("ERROR:")} {message}', file=sys.stderr)
    sys.exit(1)

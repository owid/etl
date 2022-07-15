from typing import Optional
from owid.catalog.utils import validate_underscore
from pywebio import input as pi
from pywebio import output as po
from pathlib import Path


def validate_short_name(short_name: str) -> Optional[str]:
    try:
        validate_underscore(short_name, "Short name")
        return None
    except Exception as e:
        return str(e)


def preview_file(path: Path, language: str) -> None:
    with open(path) as f:
        t = f.read()
    po.put_success(po.put_markdown(f"File `{path}` was successfully generated"))
    po.put_markdown(f"```{language}\n{t}```")

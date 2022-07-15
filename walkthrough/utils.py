from pathlib import Path
from typing import Optional

from owid.catalog.utils import validate_underscore
from pywebio import output as po


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


DUMMY_DATA = {
    "short_name": "dummy",
    "namespace": "dummy",
    "version": "2020",
    "name": "Dummy dataset",
    "description": "This\nis\na\ndummy\ndataset",
    "file_extension": "xlsx",
    "source_data_url": "https://www.rug.nl/ggdc/historicaldevelopment/maddison/data/mpd2020.xlsx",
    "publication_year": 2020,
    "source_name": "dummy source",
    "url": "https://www.dummy.com/",
}

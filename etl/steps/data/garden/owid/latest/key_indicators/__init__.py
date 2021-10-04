#
#  __init__.py
#  owid/latest/key_indicators
#

from pathlib import Path
from importlib import import_module

from owid.catalog import Dataset, DatasetMeta, Table

from etl.command import BASE_DIR


def run(dest_dir: str) -> None:
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = DatasetMeta(
        namespace="owid",
        short_name="key_indicators",
        description="The most important handful of indicators for use directly and in transforming other statistics.",
    )
    ds.save()

    # scan this folder for scripts that begin with "table_" and run them
    table_scripts = Path(__file__).parent.glob("table_*.py")
    for script in table_scripts:
        script_module = (
            script.relative_to(BASE_DIR).with_suffix("").as_posix().replace("/", ".")
        )
        t: Table = import_module(script_module).make_table()  # type: ignore
        ds.add(t)

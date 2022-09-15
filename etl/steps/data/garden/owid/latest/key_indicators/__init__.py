#
#  __init__.py
#  owid/latest/key_indicators
#

from importlib import import_module
from pathlib import Path
from typing import cast

from owid.catalog import Dataset, DatasetMeta, Table

from etl.paths import BASE_DIR


def run(dest_dir: str) -> None:
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = DatasetMeta(
        namespace="owid",
        short_name="key_indicators",
        title="Key Indicators",
        description="The most important handful of indicators for use directly and in transforming other statistics.",
    )

    # scan this folder for scripts that begin with "table_" and run them
    sources = []
    table_scripts = Path(__file__).parent.glob("table_*.py")
    for script in table_scripts:
        script_module = script.relative_to(BASE_DIR).with_suffix("").as_posix().replace("/", ".")
        t: Table = import_module(script_module).make_table()  # type: ignore
        ds.add(t)
        # Collect sources from variables
        sources.extend([source for col in t.columns for source in t[col].metadata.sources])

    # Add sources from variables (ensure sources are not duplicated)
    ds.metadata.sources = [dict(ss) for ss in set(frozenset(s.items()) for s in sources)]

    ds.save()

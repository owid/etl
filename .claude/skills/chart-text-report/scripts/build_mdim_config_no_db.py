"""Build MDim `.config.json` files locally without needing a MySQL connection.

`Collection.save()` validates indicators in the DB and upserts the config — both
require the local MySQL server. When it's down (common during editorial passes),
we can still regenerate the `.config.json` by patching those DB-touching bits to
no-ops and letting `save_config_local()` write the JSON.

Usage:
    .venv/bin/python .claude/skills/chart-text-report/scripts/build_mdim_config_no_db.py \\
        etl.steps.export.multidim.wb.latest.incomes_pip \\
        etl.steps.export.multidim.wb.latest.gini_pip

Each argument is the dotted import path of an MDim step module (a file under
`etl/steps/export/multidim/.../<name>.py`). The script imports each one and calls
its `run()` function; the `.config.json` ends up at `export/multidim/.../<name>/<name>.config.json`.
"""

from __future__ import annotations

import argparse
import importlib
from unittest.mock import patch

from etl.collection.model import core as collection_core


def _noop(*args, **kwargs):
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "step_modules",
        nargs="+",
        help="Dotted module paths of MDim step files (e.g. etl.steps.export.multidim.wb.latest.gini_pip).",
    )
    args = parser.parse_args()

    with (
        patch.object(collection_core, "validate_indicators_in_db", _noop),
        patch.object(collection_core.Collection, "upsert_to_db", _noop),
    ):
        for module_path in args.step_modules:
            print(f"\n=== Running {module_path} ===")
            module = importlib.import_module(module_path)
            module.run()


if __name__ == "__main__":
    main()

"""Guard against drift in the auto-generated etl/collection/model/schema_types.py.

The file is generated from `schemas/multidim-schema.json`, `schemas/dataset-schema.json` and the
vendored grapher schema in `schemas/` — all committed to the repo, so this check is deterministic
and offline. It fails when:
- one of the source schemas was edited without regenerating schema_types.py, or
- schema_types.py was edited by hand (hand-written types belong in etl/collection/model/params.py).

To fix a failure, run: python scripts/generate_schema_types.py
"""

import subprocess
import sys

from etl.paths import BASE_DIR


def test_schema_types_is_up_to_date():
    result = subprocess.run(
        [sys.executable, str(BASE_DIR / "scripts" / "generate_schema_types.py"), "--check"],
        capture_output=True,
        text=True,
        cwd=BASE_DIR,
    )
    assert result.returncode == 0, (
        "etl/collection/model/schema_types.py is out of date with the JSON schemas in schemas/.\n"
        "Run `python scripts/generate_schema_types.py` to regenerate it "
        "(hand-written types belong in etl/collection/model/params.py, not in the generated file).\n\n"
        f"{result.stdout}\n{result.stderr}"
    )

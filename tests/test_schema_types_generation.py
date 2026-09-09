"""Guard against drift in the auto-generated etl/collection/model/schema_types.py.

The file is generated from `schemas/multidim-schema.json`, `schemas/dataset-schema.json` and the
vendored grapher schema in `schemas/` — all committed to the repo, so this check is deterministic
and offline. It fails when:
- one of the source schemas was edited without regenerating schema_types.py, or
- schema_types.py was edited by hand (hand-written types belong in etl/collection/model/params.py).

To fix a failure, run: python scripts/generate_schema_types.py
"""

import json
import shutil
import subprocess
import sys
from unittest.mock import Mock, patch

import pytest

from etl.paths import BASE_DIR, SCHEMAS_DIR


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


def test_special_property_names_are_escaped_in_generated_annotations(tmp_path):
    """Special JSON-schema property names must not become executable Python."""
    from typing import TypedDict

    from scripts.generate_schema_types import TypedDictGenerator

    marker = tmp_path / "schema_codegen_pwned"
    malicious_name = f'poc": __import__("pathlib").Path({str(marker)!r}).write_text("pwned") #\''

    generated = TypedDictGenerator().generate_typeddict(
        class_name="ExploitConfig",
        properties={malicious_name: {"type": "string"}},
    )

    namespace = {"TypedDict": TypedDict}
    exec(generated, namespace)

    assert not marker.exists()
    assert namespace["ExploitConfig"].__annotations__[malicious_name] is str


@pytest.fixture
def sandboxed_schemas(tmp_path, monkeypatch):
    """Copy the schemas that a version bump touches into a temp dir and point both modules at it."""
    import etl.config as config
    import scripts.generate_schema_types as gen

    for name in ("multidim-schema.json", "explorer-schema.json"):
        shutil.copy(SCHEMAS_DIR / name, tmp_path / name)
    vendored = next(SCHEMAS_DIR.glob("grapher-schema.[0-9][0-9][0-9].json"))
    shutil.copy(vendored, tmp_path / vendored.name)

    monkeypatch.setattr(config, "SCHEMAS_DIR", tmp_path)
    monkeypatch.setattr(gen, "SCHEMAS_DIR", tmp_path)
    return tmp_path, vendored.name


def _fake_upstream(schema: dict, version: str) -> Mock:
    schema = json.loads(json.dumps(schema))
    schema["$id"] = f"https://files.ourworldindata.org/schemas/grapher-schema.{version}.json"
    schema["properties"]["$schema"]["const"] = schema["$id"]
    schema["properties"]["$schema"]["default"] = schema["$id"]
    resp = Mock()
    resp.json.return_value = schema
    resp.raise_for_status.return_value = None
    return resp


def test_bump_replaces_the_vendored_schema_and_repoints_refs(sandboxed_schemas):
    """`--bump-version` moves us to a newly published version without leaving the old one behind.

    The vendored file is the single source of truth for the version (`DEFAULT_GRAPHER_SCHEMA` is
    derived from it), so the bump has to swap the file *and* repoint every `$ref` that names it by
    filename. Leaving two copies on disk would break `vendored_grapher_schema_id()` for everyone.
    """
    import etl.config as config
    import scripts.generate_schema_types as gen

    tmp_path, old_name = sandboxed_schemas
    current = json.loads((tmp_path / old_name).read_text())
    refs_before = {
        name: (tmp_path / name).read_text().count(f"{old_name}#")
        for name in ("multidim-schema.json", "explorer-schema.json")
    }
    assert all(n > 0 for n in refs_before.values()), "fixture should start with $refs to the vendored schema"

    with patch("etl.http.session.get", return_value=_fake_upstream(current, "099")):
        assert gen.bump_vendored_schema() is True

    assert not (tmp_path / old_name).exists()
    assert (tmp_path / "grapher-schema.099.json").exists()
    assert config.vendored_grapher_schema_id().endswith("grapher-schema.099.json")
    for name, count in refs_before.items():
        text = (tmp_path / name).read_text()
        assert text.count(f"{old_name}#") == 0
        assert text.count("grapher-schema.099.json#") == count


def test_bump_is_a_noop_when_already_current(sandboxed_schemas):
    """Re-running `--bump-version` must not churn files — CI and the sync workflow may call it blindly."""
    import scripts.generate_schema_types as gen

    tmp_path, old_name = sandboxed_schemas
    current = json.loads((tmp_path / old_name).read_text())
    before = {p.name: p.read_text() for p in tmp_path.glob("*.json")}

    with patch("etl.http.session.get", return_value=_fake_upstream(current, old_name.split(".")[1])):
        assert gen.bump_vendored_schema() is False

    assert {p.name: p.read_text() for p in tmp_path.glob("*.json")} == before


def test_bump_refuses_a_non_versioned_latest_id(sandboxed_schemas):
    """If `latest.json` ever stops naming a concrete version, don't vendor it under a guessed name."""
    import scripts.generate_schema_types as gen

    tmp_path, old_name = sandboxed_schemas
    current = json.loads((tmp_path / old_name).read_text())
    broken = json.loads(json.dumps(current))
    broken["$id"] = "https://files.ourworldindata.org/schemas/grapher-schema.latest.json"
    resp = Mock()
    resp.json.return_value = broken
    resp.raise_for_status.return_value = None

    with patch("etl.http.session.get", return_value=resp):
        with pytest.raises(SystemExit, match="not a concrete"):
            gen.bump_vendored_schema()

    assert (tmp_path / old_name).exists()

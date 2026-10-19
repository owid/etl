"""The editor-only grapher config schema (scripts/build_editor_schema.py) and the overlays that use it."""

import importlib.util
import json

import pytest
from jsonschema import Draft7Validator
from jsonschema.protocols import Validator
from referencing import Registry, Resource

from etl.paths import BASE_DIR, SCHEMAS_DIR

_spec = importlib.util.spec_from_file_location("build_editor_schema", BASE_DIR / "scripts" / "build_editor_schema.py")
assert _spec and _spec.loader
build_editor_schema = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(build_editor_schema)

# A stand-in for the published grapher schema, with the shapes the transform has to handle.
GRAPHER_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "https://files.ourworldindata.org/schemas/grapher-schema.011.json",
    "type": "object",
    "required": ["$schema"],
    "additionalProperties": False,
    "properties": {
        "$schema": {"type": "string"},
        "title": {"type": "string"},
        "hasMapTab": {"type": "boolean"},
        "tab": {"type": "string", "enum": ["chart", "map"]},
        "yAxis": {
            "type": "object",
            "additionalProperties": False,
            "properties": {"min": {"oneOf": [{"type": "number"}, {"const": "auto"}]}},
        },
        "dimensions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "display": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {"name": {"type": "string"}},
                    }
                },
            },
        },
    },
}


def _overlay_validator(overlay: str) -> Validator:
    """Validate the way the editor does: the committed overlay, resolving `grapher-config.json` next to it."""
    editor_schema = build_editor_schema.build_editor_schema(GRAPHER_SCHEMA)
    registry = Registry().with_resource("file:///editor/grapher-config.json", Resource.from_contents(editor_schema))
    schema = json.loads((SCHEMAS_DIR / "editor" / overlay).read_text())
    schema["$id"] = f"file:///editor/{overlay}"
    return Draft7Validator(schema, registry=registry)


def _meta(grapher_config: dict) -> dict:
    return {"tables": {"t": {"variables": {"v": {"presentation": {"grapher_config": grapher_config}}}}}}


def _errors(validator: Validator, doc: dict) -> list[str]:
    return [e.message for e in validator.iter_errors(doc)]


@pytest.mark.parametrize(
    "grapher_config",
    [
        {"title": "No $schema, as in every .meta.yml"},
        {"hasMapTab": "{definitions.hasmaptab}"},
        {"tab": "<% if x %>map<% else %>chart<% endif %>"},
        {"yAxis": {"min": "<< 90 | as_value >>"}},
        {"yAxis": {"min": "auto"}},
    ],
)
def test_meta_overlay_accepts_what_etl_authors(grapher_config):
    assert _errors(_overlay_validator("dataset-grapher-config.json"), _meta(grapher_config)) == []


@pytest.mark.parametrize(
    "grapher_config",
    [{"titel": "typo"}, {"hasMapTab": "yes"}, {"tab": "mapp"}, {"yAxis": {"minn": 0}}],
)
def test_meta_overlay_rejects_mistakes(grapher_config):
    validator = _overlay_validator("dataset-grapher-config.json")
    assert _errors(validator, _meta(grapher_config))
    # `definitions.common` is checked the same way as each indicator.
    assert _errors(validator, {"definitions": {"common": {"presentation": {"grapher_config": grapher_config}}}})


def test_multidim_overlay():
    validator = _overlay_validator("multidim-grapher-config.json")

    def view(config=None, y=None):
        return {"views": [{"config": config or {}, "indicators": {"y": y or "ns/2024/ds#ind"}}]}

    assert (
        _errors(validator, view(config={"title": "ok"}, y=["ns/2024/ds#a", {"catalogPath": "x", "display": {}}])) == []
    )
    assert _errors(validator, view(config={"titel": "typo"}))
    assert _errors(validator, view(y={"catalogPath": "x", "display": {"nmae": "typo"}}))
    assert _errors(validator, view(y=[{"catalogPath": "x", "display": {"nmae": "typo"}}]))
    assert _errors(validator, {"definitions": {"common_views": [{"config": {"titel": "typo"}}]}})


def test_offline_fallback_keeps_the_overlays_resolvable(tmp_path, monkeypatch):
    """Without network, `make .venv` must not fail, and the overlays need a file to `$ref`."""
    import requests

    def _offline(*args, **kwargs):
        raise requests.ConnectionError("offline")

    monkeypatch.setattr(build_editor_schema, "OUTPUT", tmp_path / "editor" / "grapher-config.json")
    monkeypatch.setattr(build_editor_schema.session, "get", _offline)
    build_editor_schema.main()
    assert json.loads(build_editor_schema.OUTPUT.read_text()) == {"type": "object"}

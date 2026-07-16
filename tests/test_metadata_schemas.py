import json
import re
import subprocess
from pathlib import Path

import pytest
import structlog
import yaml
from jsonschema import (
    Draft7Validator,
)
from jsonschema.exceptions import ValidationError
from yaml.loader import SafeLoader

from etl.config import DEFAULT_GRAPHER_SCHEMA
from etl.dag_helpers import get_active_snapshots, get_active_steps
from etl.files import read_json_schema
from etl.paths import BASE_DIR, SCHEMAS_DIR, SNAPSHOTS_DIR, STEPS_DATA_DIR

log = structlog.get_logger()

DATASET_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "dataset-schema.json")
SNAPSHOT_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "snapshot-schema.json")


# only validate versions after this date
# bump this if we significantly change the schema
VALIDATE_AFTER = "2024-03-01"

# Excluded invalid metadata files, should be fixed if possible
EXCLUDE = [
    "garden/excess_mortality/latest/excess_mortality/excess_mortality.meta.yml",
    "meadow/who/latest/fluid.meta.yml",
]


# Override the default YAML loader to treat dates as strings
def construct_yaml_str(self, node):
    return self.construct_scalar(node)


def load_yaml_as_string(path):
    SafeLoader.add_constructor("tag:yaml.org,2002:timestamp", construct_yaml_str)
    with open(path) as file:
        return yaml.load(file, Loader=SafeLoader)


def _strip_jinja_templated_values(obj):
    """Recursively remove dict entries whose value is a Jinja-templated string.

    Used to skip schema validation for typed (non-string) fields that contain
    Jinja templates — those validate at runtime after rendering, not statically.
    Only called on `display` and `presentation.grapher_config` blocks (which
    have typed numeric fields like ``numDecimalPlaces``, ``yAxis.min``,
    ``yEquals``); string-typed fields elsewhere keep their schema coverage.
    """
    if isinstance(obj, dict):
        for key in list(obj.keys()):
            val = obj[key]
            if isinstance(val, str) and "<%" in val:
                del obj[key]
            else:
                _strip_jinja_templated_values(val)
    elif isinstance(obj, list):
        for item in obj:
            _strip_jinja_templated_values(item)


def _get_changed_files_vs_master(pattern: str) -> set[str] | None:
    """Return set of files changed vs master matching pattern, or None to validate all.

    Returns None (= validate all) if:
    - We're on master itself
    - git isn't available
    - The schema files themselves changed
    """
    try:
        # Check if we're on master
        branch = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if branch.returncode != 0 or branch.stdout.strip() == "master":
            return None

        # Check if schema files changed — if so, validate everything
        schema_diff = subprocess.run(
            ["git", "diff", "--name-only", "master", "--", "schemas/"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if schema_diff.returncode != 0 or schema_diff.stdout.strip():
            return None

        # Get changed files matching pattern
        result = subprocess.run(
            ["git", "diff", "--name-only", "master", "--", pattern],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return None

        return set(result.stdout.strip().splitlines())
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None


def _should_validate(file_path: Path, changed_files: set[str] | None) -> bool:
    """Check if a file should be validated based on the changed files set.

    `changed_files` contains repo-relative paths as returned by git (e.g.
    ``etl/steps/data/garden/foo/2024-01-01/foo.meta.yml``), so we must
    convert the absolute ``file_path`` to a repo-relative path before
    checking membership.
    """
    if changed_files is None:
        return True
    try:
        rel = str(file_path.relative_to(BASE_DIR))
    except ValueError:
        # file_path is outside the repo — fall back to str comparison
        rel = str(file_path)
    return rel in changed_files


def test_dataset_schemas():
    changed_files = _get_changed_files_vs_master("etl/steps/data/**/*.meta.yml")
    if changed_files is not None and not changed_files:
        return  # No meta.yml files changed, skip entirely

    validator = Draft7Validator(DATASET_SCHEMA)
    validation_errors = []
    active_steps = get_active_steps()

    # Walk over all files in STEPS_DATA_DIR with *.meta.yml extension
    for meta_file_path in Path(STEPS_DATA_DIR).glob("**/*.meta.yml"):
        if not _should_validate(meta_file_path, changed_files):
            continue

        # Skip files that are not part of the active DAG (archived steps)
        rel = str(meta_file_path.relative_to(STEPS_DATA_DIR)).rsplit(".meta.yml", 1)[0]
        if not any(s.startswith(rel) for s in active_steps):
            continue

        # extract version from path
        version = meta_file_path.relative_to(STEPS_DATA_DIR).parts[2]

        # Only validate versions after VALIDATE_AFTER
        if version != "latest" and version < VALIDATE_AFTER:
            continue

        # Exclude known invalid metadata files
        if any(ex in str(meta_file_path) for ex in EXCLUDE):
            continue

        # Ignore fasttrack and backport metadata
        if "fasttrack/" in str(meta_file_path) or "backport/" in str(meta_file_path):
            continue

        data = load_yaml_as_string(meta_file_path)

        # Ignore invalid `description` field, it's in too many latest datasets
        for tab in data.get("tables", {}).values():
            for ind in tab.get("variables", {}).values():
                if "description" in ind:
                    del ind["description"]

                # Ignore pinned schemas in presentation.grapher_config
                if "$schema" in ind.get("presentation", {}).get("grapher_config", {}):
                    del ind["presentation"]["grapher_config"]

                # Strip Jinja templates from the two blocks that hold typed
                # numeric fields (display: numDecimalPlaces, yAxis…; grapher_config:
                # yAxis.min/max, yEquals…). Runtime rendering + post-render schema
                # validation in `etl.grapher.helpers._validate_grapher_config`
                # catches type mismatches for those fields after Jinja resolves.
                # All other fields (description_short, title_public, etc.) keep
                # their schema coverage even when they contain Jinja, since their
                # schema type is `string` and Jinja-templated strings still pass.
                display = ind.get("display", {})
                if display:
                    for key in list(display.keys()):
                        if isinstance(display[key], str) and "<%" in display[key]:
                            del display[key]
                gc = ind.get("presentation", {}).get("grapher_config", {})
                if gc:
                    _strip_jinja_templated_values(gc)

        # Validate the loaded data against the schema
        try:
            validator.validate(data)
        except ValidationError as e:
            validation_errors.append((meta_file_path, e))

    # If there are validation errors, log summary and raise the first one
    if validation_errors:
        log.error("VALIDATION SUMMARY", error_count=len(validation_errors))
        for i, (file_path, error) in enumerate(validation_errors, 1):
            log.error("Validation error", index=i, file=str(file_path), message=error.message)

        # Raise the first error
        first_file, first_error = validation_errors[0]
        raise ValidationError(f"Validation error in file: {first_file}") from first_error


def test_snapshot_schemas():
    changed_files = _get_changed_files_vs_master("snapshots/**/*.dvc")
    if changed_files is not None and not changed_files:
        return  # No .dvc files changed, skip entirely

    validator = Draft7Validator(SNAPSHOT_SCHEMA)
    active_snapshots = get_active_snapshots()

    for meta_file_path in Path(SNAPSHOTS_DIR).glob("**/*.dvc"):
        if not _should_validate(meta_file_path, changed_files):
            continue

        # Skip files that are not part of the active DAG (archived snapshots)
        rel = str(meta_file_path.relative_to(SNAPSHOTS_DIR))
        rel_no_dvc = rel.rsplit(".dvc", 1)[0]
        if rel_no_dvc not in active_snapshots:
            continue

        # extract version from etl/snapshots/namespace/version/snapshot_name.ext.dvc
        version = meta_file_path.parent.name

        # Only validate versions after VALIDATE_AFTER
        if version != "latest" and version < VALIDATE_AFTER:
            continue

        # Ignore fasttrack and backport metadata
        if "fasttrack/" in str(meta_file_path) or "backport/" in str(meta_file_path):
            continue

        data = load_yaml_as_string(meta_file_path)

        # Validate the loaded data against the schema
        try:
            validator.validate(data)
        except ValidationError as e:
            raise ValidationError(f"Validation error in file: {meta_file_path}") from e


# Top-level (2-space indented) license keys — the deprecated SnapshotMeta-level location. This
# covers `meta.license` and the legacy `meta.license_name` / `meta.license_url`, which
# `SnapshotMeta.load_from_yaml` also funnels into the top-level `SnapshotMeta.license`. The
# current convention is `meta.origin.license` (4-space).
_TOP_LEVEL_LICENSE_KEYS = ("license", "license_name", "license_url")
_TOP_LEVEL_LICENSE_RE = re.compile(r"^  license(_name|_url)?:", re.MULTILINE)


def test_snapshot_license_lives_under_origin():
    """Guardrail: an origin-based snapshot must declare its license under `meta.origin.license`,
    never as a top-level `meta.license` (nor the legacy `meta.license_name` / `meta.license_url`).

    All spots parse without error, but they behave differently: the top-level fields populate the
    dataset/variable license yet leave `origin.license` empty, so the license doesn't travel with
    the origin (it's dropped from Grapher's per-origin metadata, which matters for multi-origin
    datasets). See CLAUDE.md.

    We check key *presence* (not value) so an explicit `license: null` is caught too, keeping this
    aligned with the `not` constraint in snapshot-schema.json.

    fasttrack and backport snapshots are auto-generated (like in the schema tests above) and
    excluded. Legacy `source:`-based snapshots (no `origin`) keep their own license location.
    """
    violations = []
    for meta_file_path in Path(SNAPSHOTS_DIR).glob("**/*.dvc"):
        rel = str(meta_file_path.relative_to(SNAPSHOTS_DIR))
        if "fasttrack/" in rel or "backport/" in rel:
            continue

        text = meta_file_path.read_text()
        # Fast prefilter: skip files without any top-level license key.
        if not _TOP_LEVEL_LICENSE_RE.search(text):
            continue

        meta = (yaml.safe_load(text) or {}).get("meta") or {}
        # Only origin-based snapshots are in scope; legacy source-based ones have no origin.
        if "origin" not in meta:
            continue
        if any(key in meta for key in _TOP_LEVEL_LICENSE_KEYS):
            violations.append(rel)

    assert not violations, (
        "These snapshots set a top-level license (`meta.license` / `meta.license_name` / "
        "`meta.license_url`) on an origin-based snapshot. Move it under `meta.origin.license`:\n  "
        + "\n  ".join(sorted(violations))
    )


# Properties that only exist in the local dataset schema (not in upstream grapher schema).
# These are ETL-specific and intentionally absent from upstream.
LOCAL_ONLY_PROPERTIES = {"data", "includedEntities"}

# Vendored copy of the upstream grapher schema (see scripts/generate_schema_types.py --refresh).
VENDORED_GRAPHER_SCHEMA = SCHEMAS_DIR / DEFAULT_GRAPHER_SCHEMA.rsplit("/", 1)[-1]


def _local_enum_values(node) -> set | None:
    """Return the set of enum values a local schema node accepts, or None if unconstrained.

    Handles the local Jinja escape-hatch convention, where an upstream enum is wrapped as
    `oneOf: [{enum: [...]}, {type: string, pattern: "{definitions"}]` — the pattern branch only
    accepts Jinja/definitions strings, so plain values must still be listed in the enum branch.
    """
    if not isinstance(node, dict):
        return None
    if "enum" in node:
        return set(node["enum"])
    values = set()
    has_enum_branch = False
    for branch in node.get("oneOf", []) + node.get("anyOf", []):
        if isinstance(branch, dict) and "enum" in branch:
            has_enum_branch = True
            values |= set(branch["enum"])
    return values if has_enum_branch else None


def _find_enum_drift(upstream_node, local_node, path="grapher_config") -> list[str]:
    """Recursively compare enums between the upstream grapher schema and the local copy.

    Returns drift messages for every upstream enum value that the local schema would reject.
    Local relaxations (e.g. a field loosened to plain `string`) are skipped, and the local
    schema is allowed to accept extra values (e.g. `WorldMap` in chartTypes).
    """
    errors = []
    if not isinstance(upstream_node, dict) or not isinstance(local_node, dict):
        return errors

    if "enum" in upstream_node:
        local_values = _local_enum_values(local_node)
        if local_values is not None:
            missing = set(upstream_node["enum"]) - local_values
            if missing:
                errors.append(f"{path}: local enum is missing upstream values {sorted(missing)}")

    for key, up_child in upstream_node.get("properties", {}).items():
        loc_child = local_node.get("properties", {}).get(key)
        if loc_child is not None:
            errors += _find_enum_drift(up_child, loc_child, f"{path}.{key}")

    if "items" in upstream_node and "items" in local_node:
        errors += _find_enum_drift(upstream_node["items"], local_node["items"], f"{path}[]")

    return errors


def _load_embedded_grapher_config() -> dict:
    with open(SCHEMAS_DIR / "dataset-schema.json") as f:
        dataset_schema = json.load(f)
    return dataset_schema["properties"]["tables"]["additionalProperties"]["properties"]["variables"][
        "additionalProperties"
    ]["properties"]["presentation"]["properties"]["grapher_config"]


def test_grapher_config_schema_sync():
    """Verify that the grapher_config block embedded in dataset-schema.json stays in sync with
    the vendored grapher schema. Detects new properties we're missing and (recursively) enum
    values the embedded copy would reject — e.g. a new chart type added upstream.

    We maintain a local copy of grapher_config properties (rather than using $ref)
    because our meta.yml files use Jinja templates and {definitions...} references
    that the strict upstream schema would reject.

    This test compares against the VENDORED copy in schemas/, so it is offline and
    deterministic. Staleness of the vendored copy itself vs the live upstream is covered by
    `test_vendored_grapher_schema_is_current` (integration).
    """
    local_gc = _load_embedded_grapher_config()
    local_props = set(local_gc["properties"].keys())

    with open(VENDORED_GRAPHER_SCHEMA) as f:
        upstream = json.load(f)
    upstream_props = set(upstream["properties"].keys())

    # Check for upstream properties missing locally
    missing = upstream_props - local_props - LOCAL_ONLY_PROPERTIES
    if missing:
        # Build JSON snippets for each missing property so the developer can copy-paste
        snippets = []
        for prop in sorted(missing):
            defn = upstream["properties"][prop]
            snippet = json.dumps({prop: defn}, indent=2)
            # Indent to match the nesting level in dataset-schema.json
            snippet = "\n".join(" " * 24 + line for line in snippet.strip("{}").strip().split("\n"))
            snippets.append(snippet)

        raise AssertionError(
            f"Vendored grapher schema ({VENDORED_GRAPHER_SCHEMA.name}) has properties missing from\n"
            f"schemas/dataset-schema.json → grapher_config: {sorted(missing)}\n"
            f"\n"
            f"To fix, add these properties inside the 'grapher_config.properties' object in\n"
            f"schemas/dataset-schema.json (before the 'additionalProperties' key):\n"
            f"\n" + "\n".join(snippets) + "\n\n"
            "NOTE: If a property uses a strict type (e.g. enum, array) but our meta.yml files\n"
            "use Jinja templates in that field, relax the type to 'string' or use\n"
            "oneOf/anyOf to accept both. If the property is ETL-only and intentionally absent\n"
            "from upstream, add it to LOCAL_ONLY_PROPERTIES in this test file."
        )

    # Check that enum values inside shared properties haven't drifted (e.g. a new chart type
    # added to `tab` or `sortBy` upstream that the embedded copy would reject).
    drift = []
    for prop, up_node in upstream["properties"].items():
        loc_node = local_gc["properties"].get(prop)
        if loc_node is not None:
            drift += _find_enum_drift(up_node, loc_node, f"grapher_config.{prop}")
    assert not drift, (
        "Enum values in schemas/dataset-schema.json → grapher_config have drifted from the\n"
        f"vendored grapher schema ({VENDORED_GRAPHER_SCHEMA.name}):\n  "
        + "\n  ".join(drift)
        + "\n\nSync the listed enums (see the /sync-grapher-schema skill)."
    )

    # Check for local properties removed from upstream (excluding known local-only ones)
    removed = (local_props - LOCAL_ONLY_PROPERTIES) - upstream_props
    if removed:
        log.warning(
            "Local grapher_config has properties not in upstream schema (may be deprecated)",
            properties=sorted(removed),
        )


@pytest.mark.integration
def test_vendored_grapher_schema_is_current():
    """Verify that the vendored grapher schema matches the live upstream one.

    Upstream mutates the schema in place without version bumps (e.g. dumbbell plots landed in
    grapher-schema.010.json directly), so this is the automatic watch for upstream changes.
    Network-dependent by nature, hence integration-marked. To fix a failure, run
    `python scripts/generate_schema_types.py --refresh` and review the diff
    (see the /sync-grapher-schema skill).
    """
    from etl.http import session

    resp = session.get(DEFAULT_GRAPHER_SCHEMA, timeout=30)
    resp.raise_for_status()
    with open(VENDORED_GRAPHER_SCHEMA) as f:
        vendored = json.load(f)
    assert vendored == resp.json(), (
        f"Vendored {VENDORED_GRAPHER_SCHEMA.name} is stale vs {DEFAULT_GRAPHER_SCHEMA}.\n"
        "Run `python scripts/generate_schema_types.py --refresh` and follow /sync-grapher-schema."
    )


@pytest.mark.integration
def test_no_newer_grapher_schema_version():
    """Detect when upstream publishes a NEW grapher schema version.

    `grapher-schema.latest.json` carries an `$id` naming the concrete version it points to;
    when that moves past DEFAULT_GRAPHER_SCHEMA, we should consider bumping. See the
    "Version bump" section of the /sync-grapher-schema skill.
    """
    from etl.http import session

    latest_url = DEFAULT_GRAPHER_SCHEMA.rsplit("/", 1)[0] + "/grapher-schema.latest.json"
    resp = session.get(latest_url, timeout=30)
    resp.raise_for_status()
    latest_id = resp.json().get("$id")
    assert latest_id == DEFAULT_GRAPHER_SCHEMA, (
        f"Upstream published a newer grapher schema: {latest_id} "
        f"(we pin {DEFAULT_GRAPHER_SCHEMA}).\n"
        "Follow the 'Version bump' section of the /sync-grapher-schema skill to upgrade."
    )

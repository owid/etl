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
from etl.paths import BASE_DIR, SCHEMAS_DIR, SNAPSHOTS_DIR, STEP_DIR, STEPS_DATA_DIR

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


def _strip_jinja_from_typed_blocks(ind):
    """Strip Jinja templates from the two blocks of an indicator that hold typed fields.

    `display` (numDecimalPlaces, timeInterval, …) and
    `presentation.grapher_config` (yAxis.min/max, yEquals, …) declare enums and
    numbers, which a Jinja template never satisfies statically. Those validate
    at runtime once the dimensions are known — `_expand_jinja` renders them, and the
    grapher admin API validates grapher_config when the indicator is upserted.
    Every other field (description_short, title_public, …) is typed
    `string` in the schema, so a Jinja template passes and keeps its coverage.

    Applied to each indicator *and* to `definitions.common`, which the schema
    `$ref`s to the very same indicator definition (#6674).
    """
    if not isinstance(ind, dict):
        return
    _strip_jinja_templated_values(ind.get("display"))
    presentation = ind.get("presentation")
    if isinstance(presentation, dict):
        _strip_jinja_templated_values(presentation.get("grapher_config"))


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
    validated_count = 0
    active_steps = get_active_steps()

    # Walk over all files in STEPS_DATA_DIR with *.meta.yml extension
    for meta_file_path in Path(STEPS_DATA_DIR).glob("**/*.meta.yml"):
        if not _should_validate(meta_file_path, changed_files):
            continue

        # Skip files that are not part of the active DAG (archived steps). Match the whole step
        # path: a prefix test against the version directory would also accept archived datasets
        # that share it with an active one (`garden/covid/latest/ecdc` next to the active
        # `garden/covid/latest/cases_deaths`). The `/` form covers steps whose files live in
        # their own directory, e.g. `garden/owid/latest/key_indicators/key_indicators.meta.yml`.
        rel = str(meta_file_path.relative_to(STEPS_DATA_DIR)).rsplit(".meta.yml", 1)[0]
        if not any(rel == step or rel.startswith(step + "/") for step in active_steps):
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

        # `definitions.common` is merged into every indicator, and the schema validates it
        # against the same indicator definition, so its typed blocks need the same treatment.
        definitions = data.get("definitions")
        if isinstance(definitions, dict):
            _strip_jinja_from_typed_blocks(definitions.get("common"))

        # Ignore invalid `description` field, it's in too many latest datasets
        for tab in data.get("tables", {}).values():
            for ind in tab.get("variables", {}).values():
                if "description" in ind:
                    del ind["description"]

                # Ignore pinned schemas in presentation.grapher_config
                if "$schema" in ind.get("presentation", {}).get("grapher_config", {}):
                    del ind["presentation"]["grapher_config"]

                _strip_jinja_from_typed_blocks(ind)

        # Validate the loaded data against the schema
        validated_count += 1
        try:
            validator.validate(data)
        except ValidationError as e:
            validation_errors.append((meta_file_path, e))

    # A full scan must reach the validator, otherwise a broken filter makes this test a no-op that
    # passes for three months (#6572). The branch fast path is exempt: a PR may legitimately touch
    # only files that every other filter then skips.
    if changed_files is None:
        assert validated_count > 0, (
            "test_dataset_schemas validated 0 files on a full scan — the active-DAG filter is "
            "skipping everything, so this test is not checking any metadata."
        )

    # If there are validation errors, log summary and raise the first one
    if validation_errors:
        log.error("VALIDATION SUMMARY", error_count=len(validation_errors))
        for i, (file_path, error) in enumerate(validation_errors, 1):
            log.error("Validation error", index=i, file=str(file_path), message=error.message)

        # Raise the first error
        first_file, first_error = validation_errors[0]
        raise ValidationError(f"Validation error in file: {first_file}") from first_error


def test_jinja_in_typed_fields_is_stripped_from_definitions_common():
    """A Jinja-templated enum/numeric field must be skipped in `definitions.common` too.

    `definitions.common` `$ref`s the indicator definition, so its `display` and
    `presentation.grapher_config` carry the same enums and numbers a template can't
    satisfy statically. Only `tables.*.variables.*` used to be sanitized, so the
    first such template on master (`timeInterval` in covid/2024-11-05/github_stats,
    #6613) broke every build until #6674.
    """
    data = {
        "definitions": {
            "common": {
                # Renders to `week` / `day`, both in the schema's enum, once `interval` is known.
                "display": {"timeInterval": '<% if interval == "weekly" %>week<% else %>day<% endif %>'},
            }
        },
        "tables": {},
    }
    _strip_jinja_from_typed_blocks(data["definitions"]["common"])
    Draft7Validator(DATASET_SCHEMA).validate(data)

    # Guardrail: a non-templated bad value must still fail, i.e. the block is sanitized, not skipped.
    bad = {"definitions": {"common": {"display": {"timeInterval": "fortnight"}}}, "tables": {}}
    _strip_jinja_from_typed_blocks(bad["definitions"]["common"])
    with pytest.raises(ValidationError):
        Draft7Validator(DATASET_SCHEMA).validate(bad)


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


def test_multidim_configs_pin_grapher_schema():
    """Guardrail: every multidim collection config must pin `grapher_schema`.

    Grapher injects the collection's `grapherConfigSchema` as the `$schema` of each view config and
    migrates outdated configs forward. When no version is given, ETL falls back to
    `DEFAULT_GRAPHER_SCHEMA`, i.e. it tells Grapher the configs are already current — so a breaking
    schema change upstream would silently skip the migration for those views.

    Pinning records the version the config was actually authored against. See
    `etl.collection.utils.resolve_grapher_schema` for the accepted forms.
    """
    missing = []
    for config_path in sorted(Path(STEP_DIR / "export" / "multidim").glob("**/*.yml")):
        config = yaml.safe_load(config_path.read_text()) or {}
        # Only collection configs are in scope; the directory also holds plain data yaml
        # (e.g. un/latest/map_brackets.yml).
        if not isinstance(config, dict) or not {"dimensions", "views"} <= set(config):
            continue
        if "grapher_schema" not in config:
            missing.append(str(config_path.relative_to(BASE_DIR)))

    assert not missing, (
        "These multidim configs don't pin `grapher_schema`. Add the current version as a "
        f'**quoted** string (an unquoted `011` is parsed as octal) — `grapher_schema: "'
        f'{DEFAULT_GRAPHER_SCHEMA.rsplit(".", 2)[1]}"`:\n  ' + "\n  ".join(missing)
    )


@pytest.mark.integration
def test_no_newer_grapher_schema_version():
    """Detect when upstream publishes a NEW grapher schema version.

    `grapher-schema.latest.json` carries an `$id` naming the concrete version it points to;
    when that moves past DEFAULT_GRAPHER_SCHEMA, we should consider bumping: the pin is
    what grapher's config migrations key on.
    """
    from etl.http import session

    latest_url = DEFAULT_GRAPHER_SCHEMA.rsplit("/", 1)[0] + "/grapher-schema.latest.json"
    resp = session.get(latest_url, timeout=30)
    resp.raise_for_status()
    latest_id = resp.json().get("$id")
    assert latest_id == DEFAULT_GRAPHER_SCHEMA, (
        f"Upstream published a newer grapher schema: {latest_id} "
        f"(we pin {DEFAULT_GRAPHER_SCHEMA}).\n"
        "Bump DEFAULT_GRAPHER_SCHEMA in etl/config.py once grapher renders the new version."
    )

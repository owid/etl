"""Field-build tracing: map a rendered metadata field back to its editable source.

The value the DB serves is the *output* of a three-phase build:

- Phase A — dynamic-yaml resolves ``{definitions.xxx}`` references and YAML anchors
  at load time (plus ``shared.meta.yml`` merging).
- Phase B — precedence merge: ``definitions.common`` < ``tables.<t>.common`` <
  ``tables.<t>.variables.<v>`` (``presentation``/``grapher_config`` deep-merged).
- Phase C — Jinja (``<% %>`` / ``<< >>`` delimiters) rendered per expanded column
  with a dimensions-only context (``etl/grapher/helpers.py::_metadata_for_dimensions``).

MDim configs use anchors + ``{definitions.xxx}`` and a dimension-matched
``definitions.common_views`` merge, but no Jinja; views may also be generated
programmatically in the step ``.py`` (``str.format`` params via ``fill_placeholders``).

The tracer replays these builds so the export can say exactly what to edit, and
verifies itself by re-rendering the traced template and comparing against the
value the suggestion was filed on (``render_verified``).
"""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import structlog
from owid.catalog.core import jinja
from owid.catalog.core.meta import description_key_to_string
from owid.catalog.core.utils import dynamic_yaml_load, dynamic_yaml_to_dict, underscore
from owid.catalog.core.yaml_metadata import merge_with_shared_meta
from sqlalchemy.orm import Session

import etl.grapher.model as gm
from apps.metadata_review.resolution import dimensions_to_view_id
from etl import paths
from etl.dag_helpers import load_dag
from etl.files import ruamel_load

log = structlog.get_logger()

# Indicator fieldPath -> key path inside a variable metadata block.
FIELD_KEY_PATHS = {
    "grapher_config.title": ["presentation", "grapher_config", "title"],
    "grapher_config.subtitle": ["presentation", "grapher_config", "subtitle"],
    "grapher_config.note": ["presentation", "grapher_config", "note"],
    "description_short": ["description_short"],
    "description_key": ["description_key"],
}


@dataclass
class EditCandidate:
    """One place where the suggested change can be applied."""

    # Repo-relative file path.
    file: str
    # Dotted key path inside the file (None when the target is a .py).
    yaml_path: str | None
    # literal | jinja | common-block | mdim-page | mdim-view | mdim-common-views |
    # grapher-step-override | programmatic
    kind: str
    # Raw, unrendered template text (Jinja / {definitions.*} / {params} intact).
    template: str | None = None
    # Jinja dimension context used to render this indicator's value.
    render_context: dict[str, Any] | None = None
    # The YAML block that supplies the value (Phase-B precedence winner).
    supplied_by: str | None = None
    # True when re-rendering `template` reproduces the suggestion's current value;
    # None when the replay could not run.
    render_verified: bool | None = None
    exact_key_found: bool = True
    generated: bool = False
    # Other variables / views affected by editing this source.
    shared_with: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass
class FieldTrace:
    target_type: str
    target_path: str
    view_id: str | None
    field_path: str
    # Ordered: preferred edit location first.
    edits: list[EditCandidate] = field(default_factory=list)

    @property
    def preferred(self) -> EditCandidate | None:
        return self.edits[0] if self.edits else None


def _norm(value: Any) -> str | None:
    if value is None:
        return None
    return str(value).strip()


def _dig(d: Any, keys: list[str]) -> Any:
    for key in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(key)
        if d is None:
            return None
    return d


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(paths.BASE_DIR))
    except ValueError:
        return str(path)


# ---------------------------------------------------------------------------
# Indicator fields (garden / grapher .meta.yml)
# ---------------------------------------------------------------------------


def trace_indicator_field(
    session: Session,
    catalog_path: str,
    field_path: str,
    current_value: str | None,
) -> FieldTrace:
    """Trace an indicator-level field back to the .meta.yml (or .py) that builds it."""
    trace = FieldTrace(target_type="indicator", target_path=catalog_path, view_id=None, field_path=field_path)
    assert field_path in FIELD_KEY_PATHS, f"Unknown indicator field path: {field_path}"

    # 1. The stored back-trace key: base variable name + Jinja dimension context.
    variable = gm.Variable.from_catalog_path(session, catalog_path)
    path_part, column = catalog_path.split("#", 1)
    channel, namespace, version, dataset, table_name = path_part.split("/")
    assert channel == "grapher", f"Expected a grapher catalog path, got: {catalog_path}"
    dims = variable.dimensions or {}
    original_short_name = dims.get("originalShortName") or column
    dim_dict = {f["name"]: f["value"] for f in dims.get("filters", [])}

    # 2. Candidate .meta.yml files: grapher-step override first (it wins when present),
    #    then the garden step(s) resolved through the DAG.
    grapher_meta = paths.STEPS_GRAPHER_DIR / namespace / version / f"{dataset}.meta.yml"
    garden_metas = _garden_meta_files(namespace, version, dataset)

    for meta_path, is_grapher_step in [(grapher_meta, True)] + [(p, False) for p in garden_metas]:
        if not meta_path.exists():
            continue
        candidate = _trace_in_meta_yaml(
            meta_path,
            table_name=table_name,
            var_name=original_short_name,
            field_path=field_path,
            dim_dict=dim_dict,
            current_value=current_value,
        )
        if candidate is None:
            continue
        if is_grapher_step:
            candidate.kind = "grapher-step-override"
            candidate.notes.append(
                "This grapher-step .meta.yml layers on top of the garden metadata — "
                "edit here, or remove the override so the garden value applies."
            )
        trace.edits.append(candidate)

    if not trace.edits:
        # Not defined in any YAML: the field is set programmatically in the step code.
        garden_py = [p.parent / (p.name.removesuffix(".meta.yml") + ".py") for p in garden_metas] or [
            paths.STEPS_GARDEN_DIR / namespace / version / f"{dataset}.py"
        ]
        for py in garden_py:
            trace.edits.append(
                EditCandidate(
                    file=_rel(py),
                    yaml_path=None,
                    kind="programmatic",
                    render_context=dim_dict or None,
                    exact_key_found=False,
                    generated=True,
                    notes=[
                        f"`{field_path}` is not defined in any .meta.yml for `{original_short_name}` — "
                        "it is most likely set programmatically in the step code (or the .meta.yml "
                        "could not be located). Search the step for the field assignment."
                    ],
                )
            )
    return trace


def _garden_meta_files(namespace: str, version: str, dataset: str) -> list[Path]:
    """Garden .meta.yml candidates for a grapher dataset, resolved through the DAG."""
    dag = load_dag()
    deps = None
    for prefix in ("data://", "data-private://"):
        deps = dag.get(f"{prefix}grapher/{namespace}/{version}/{dataset}")
        if deps:
            break
    candidates = []
    for dep in sorted(deps or []):
        match = re.match(r"data(?:-private)?://garden/([^/]+)/([^/]+)/(.+)", dep)
        if match:
            garden_ns, garden_version, garden_short = match.groups()
            candidates.append(paths.STEPS_GARDEN_DIR / garden_ns / garden_version / f"{garden_short}.meta.yml")
    # Prefer the garden step matching the dataset short name.
    candidates.sort(key=lambda p: (p.stem.removesuffix(".meta") != dataset, str(p)))
    if not candidates:
        # No DAG entry (e.g. archived) — fall back to the conventional location.
        candidates = [paths.STEPS_GARDEN_DIR / namespace / version / f"{dataset}.meta.yml"]
    return candidates


def _trace_in_meta_yaml(
    meta_path: Path,
    table_name: str,
    var_name: str,
    field_path: str,
    dim_dict: dict[str, Any],
    current_value: str | None,
) -> EditCandidate | None:
    """Locate `field_path` for one variable inside one .meta.yml; None when absent."""
    keys = FIELD_KEY_PATHS[field_path]
    try:
        annot = dynamic_yaml_to_dict(dynamic_yaml_load(merge_with_shared_meta(meta_path)))
    except Exception as e:
        log.warning("metadata_review.trace.yaml_load_failed", path=str(meta_path), error=str(e))
        return None

    table_annot = (annot.get("tables") or {}).get(table_name) or {}
    variables = table_annot.get("variables") or {}
    # Phase-B precedence, highest first: the first block defining the leaf wins.
    blocks = [
        (f"tables.{table_name}.variables.{var_name}", variables.get(var_name) or {}),
        (f"tables.{table_name}.common", table_annot.get("common") or {}),
        ("definitions.common", (annot.get("definitions") or {}).get("common") or {}),
    ]
    supplied_by, template = None, None
    for block_name, block in blocks:
        value = _dig(block, keys)
        if value is not None:
            supplied_by, template = block_name, value
            break
    if supplied_by is None:
        return None

    template_text = template if isinstance(template, str) else str(template)
    is_jinja = "<%" in template_text or "<<" in template_text
    kind = "jinja" if is_jinja else ("common-block" if supplied_by.endswith("common") else "literal")

    candidate = EditCandidate(
        file=_rel(meta_path),
        yaml_path=f"{supplied_by}.{'.'.join(keys)}",
        kind=kind,
        template=template_text,
        render_context=dim_dict or None,
        supplied_by=supplied_by,
    )

    if is_jinja:
        candidate.notes.append(
            "Jinja template rendered with the dimension context above. When changing wording, "
            "splice dimension words into the existing sentence rather than duplicating "
            "`<% if %>` branches."
        )

    _annotate_references(candidate, meta_path, table_name, var_name, keys, template)
    _collect_shared_with(candidate, supplied_by, variables, var_name, keys, template)
    _verify_render(candidate, blocks, keys, field_path, dim_dict, var_name, current_value)
    return candidate


def _annotate_references(
    candidate: EditCandidate,
    meta_path: Path,
    table_name: str,
    var_name: str,
    keys: list[str],
    resolved_template: Any,
) -> None:
    """Detect `{definitions.*}` references / anchors / shared.meta.yml indirection.

    Compares the raw file (ruamel: anchors resolved by YAML, `{definitions.*}` strings
    intact) against the dynamic-yaml value at the same key path.
    """
    try:
        with open(meta_path) as f:
            raw = ruamel_load(f)
    except Exception:
        return
    block_keys = candidate.supplied_by.split(".") if candidate.supplied_by else []
    raw_value = _dig(raw, block_keys + keys)
    if raw_value is None:
        # Present after merging but absent from the raw file: comes from shared.meta.yml.
        shared = meta_path.parent / "shared.meta.yml"
        if shared.exists():
            candidate.notes.append(f"Value is defined in `{_rel(shared)}` (merged via shared definitions).")
            candidate.file = _rel(shared)
        return
    raw_text = raw_value if isinstance(raw_value, str) else str(raw_value)
    refs = re.findall(r"\{definitions\.([a-zA-Z0-9_.]+)\}", raw_text)
    if refs:
        candidate.template = raw_text
        candidate.notes.append(
            f"The raw value references {', '.join(f'`definitions.{r}`' for r in sorted(set(refs)))} — "
            "editing those definitions affects every field referencing them."
        )
    # A value identical to a `definitions:` entry usually means a YAML anchor.
    definitions = raw.get("definitions") or {}
    for def_key, def_value in definitions.items():
        if def_key == "common":
            continue
        if def_value == raw_value or (isinstance(def_value, dict) and _dig(def_value, keys) == raw_value):
            candidate.notes.append(
                f"Value matches `definitions.{def_key}` — likely shared via a YAML anchor; "
                "editing the anchor affects every referent."
            )
            break


def _collect_shared_with(
    candidate: EditCandidate,
    supplied_by: str,
    variables: dict[str, Any],
    var_name: str,
    keys: list[str],
    template: Any,
) -> None:
    """Enumerate the other variables affected by editing this source."""
    if supplied_by.endswith("common"):
        # A common block applies to every variable that doesn't override the leaf itself.
        candidate.shared_with = sorted(
            name for name, block in variables.items() if name != var_name and _dig(block or {}, keys) is None
        )
        if candidate.shared_with:
            candidate.notes.append("This common block also supplies the field for the variables in shared_with.")
    else:
        candidate.shared_with = sorted(
            name for name, block in variables.items() if name != var_name and _dig(block or {}, keys) == template
        )


def _verify_render(
    candidate: EditCandidate,
    blocks: list[tuple[str, dict[str, Any]]],
    keys: list[str],
    field_path: str,
    dim_dict: dict[str, Any],
    var_name: str,
    current_value: str | None,
) -> None:
    """Replay Phase C (Jinja with the dimension context) on the traced template
    and compare with the live value.

    Renders only the traced field — a whole-VariableMeta render would fail on
    *other* fields whose Jinja needs dimensions this indicator doesn't have.
    """
    if current_value is None:
        return
    try:
        # The winning block's raw value (Jinja intact), pre-Phase-C.
        template = None
        for _, block in blocks:
            template = _dig(block, keys)
            if template is not None:
                break
        if isinstance(template, list):
            items = [jinja._expand_jinja_text(str(item), dim_dict) for item in template]
            value = description_key_to_string([str(item) for item in items if item])
        else:
            value = jinja._expand_jinja_text(str(template), dim_dict)
        candidate.render_verified = _norm(value) == _norm(current_value)
        if candidate.render_verified is False:
            candidate.notes.append(
                "Re-rendering the traced template did NOT reproduce the live value — the value may "
                "have moved (e.g. edited since the suggestion) or the trace may be incomplete. "
                "Double-check before editing."
            )
    except Exception as e:
        candidate.render_verified = None
        candidate.notes.append(f"Render replay could not run ({e}).")


# ---------------------------------------------------------------------------
# MDim fields (export step config .yml / .py)
# ---------------------------------------------------------------------------


def trace_mdim_field(
    target_path: str,
    view_id: str | None,
    field_path: str,
    current_value: str | None,
) -> FieldTrace:
    """Trace an MDim page-level or view-override field to the export step config."""
    trace = FieldTrace(target_type="mdim", target_path=target_path, view_id=view_id, field_path=field_path)
    namespace, version, short = target_path.split("#")[0].split("/")
    step_dir = paths.STEP_DIR / "export" / "multidim" / namespace / version
    step_py = step_dir / f"{short}.py"

    config_path, ambiguity = _find_mdim_config(step_dir, short)
    if config_path is None:
        trace.edits.append(_generated_mdim_edit(step_py, field_path, [ambiguity] if ambiguity else []))
        return trace

    try:
        config = dynamic_yaml_to_dict(dynamic_yaml_load(config_path))
    except Exception as e:
        trace.edits.append(_generated_mdim_edit(step_py, field_path, [f"Could not parse {_rel(config_path)}: {e}"]))
        return trace

    if view_id is None:
        candidate = _trace_mdim_page_field(config, config_path, field_path)
    else:
        candidate = _trace_mdim_view_field(config, config_path, view_id, field_path)

    if candidate is None:
        notes = [
            f"`{field_path}` was not found in `{_rel(config_path)}` — the view/label is generated "
            "programmatically in the step code."
        ]
        notes.extend(_py_call_sites(step_py))
        candidate = _generated_mdim_edit(step_py, field_path, notes)
    else:
        if ambiguity:
            candidate.notes.append(ambiguity)
        if current_value is not None and candidate.template is not None:
            candidate.render_verified = _norm(candidate.template) == _norm(current_value)
            if candidate.render_verified is False and re.search(r"\{[a-zA-Z0-9_.]+\}", candidate.template):
                candidate.notes.append(
                    "The raw value contains `{placeholder}` params filled by the step code "
                    "(str.format, not Jinja) — the rendered value differs from the template."
                )
            elif candidate.render_verified is False:
                candidate.notes.append(
                    "The YAML value does not match the live value — the config may have changed "
                    "since the suggestion, or the step code post-processes this field."
                )
    trace.edits.append(candidate)
    return trace


def _find_mdim_config(step_dir: Path, short: str) -> tuple[Path | None, str | None]:
    exact = step_dir / f"{short}.config.yml"
    if exact.exists():
        return exact, None
    matches = sorted(step_dir.glob(f"{short}*.yml"))
    if len(matches) == 1:
        return matches[0], None
    if matches:
        return matches[0], (
            f"Several config files match `{short}*.yml` in `{_rel(step_dir)}`: "
            f"{', '.join(m.name for m in matches)} — traced the first; verify."
        )
    return None, f"No config .yml matching `{short}*` found in `{_rel(step_dir)}`."


def _generated_mdim_edit(step_py: Path, field_path: str, notes: list[str]) -> EditCandidate:
    return EditCandidate(
        file=_rel(step_py),
        yaml_path=None,
        kind="programmatic",
        exact_key_found=False,
        generated=True,
        notes=notes or [f"`{field_path}` is set programmatically — inspect the step code."],
    )


def _py_call_sites(step_py: Path) -> list[str]:
    """Line hints for the places a step .py typically sets view config/metadata."""
    if not step_py.exists():
        return [f"Step file `{_rel(step_py)}` not found."]
    hints = []
    markers = ("common_views", "edit_views", "group_views", "view_config", "view_metadata", "set_global_config")
    for i, line in enumerate(step_py.read_text().splitlines(), start=1):
        if any(marker in line for marker in markers):
            hints.append(f"{_rel(step_py)}:{i}: {line.strip()[:100]}")
    return hints[:15]


def _trace_mdim_page_field(config: dict[str, Any], config_path: Path, field_path: str) -> EditCandidate | None:
    """Page-level fields: title.title / title.title_variant / dimensions.<dim>...."""
    parts = field_path.split(".")
    if parts[0] == "title":
        value = _dig(config, ["title", parts[1]])
        if value is None and parts[1] == "title_variant":
            # DB key is camelized from title_variant; some configs use topic_tags-style keys.
            value = _dig(config, ["title", "titleVariant"])
        if value is None:
            return None
        return EditCandidate(
            file=_rel(config_path),
            yaml_path=f"title.{parts[1]}",
            kind="mdim-page",
            template=str(value),
        )

    # dimensions.<dim_slug>.name or dimensions.<dim_slug>.choices.<choice_slug>.{name,description}
    dim_slug = parts[1]
    for i, dim in enumerate(config.get("dimensions") or []):
        if underscore(str(dim.get("slug", "")), validate=False) != dim_slug:
            continue
        if len(parts) == 3:  # .name
            if dim.get("name") is None:
                return None
            return EditCandidate(
                file=_rel(config_path),
                yaml_path=f"dimensions[{i}].name",
                kind="mdim-page",
                template=str(dim.get("name")),
            )
        choice_slug, leaf = parts[3], parts[4]
        for j, choice in enumerate(dim.get("choices") or []):
            if underscore(str(choice.get("slug", "")), validate=False) != choice_slug:
                continue
            if choice.get(leaf) is None:
                return None
            return EditCandidate(
                file=_rel(config_path),
                yaml_path=f"dimensions[{i}].choices[{j}].{leaf}",
                kind="mdim-page",
                template=str(choice.get(leaf)),
            )
    return None


def _trace_mdim_view_field(
    config: dict[str, Any], config_path: Path, view_id: str, field_path: str
) -> EditCandidate | None:
    """View-level overrides: literal `views:` entry first, then `definitions.common_views`."""
    section, leaf = field_path.split(".", 1)  # e.g. ("config", "subtitle")
    view_dims: dict[str, str] = dict(pair.split("=", 1) for pair in view_id.split("__") if "=" in pair)

    # 1. Literal views: entry whose (normalized) dimensions match the view id.
    for i, view in enumerate(config.get("views") or []):
        dims = {
            underscore(str(k), validate=False): underscore(str(v), validate=False)
            for k, v in (view.get("dimensions") or {}).items()
        }
        if dimensions_to_view_id(dims) != view_id:
            continue
        # Authoring format is snake_case; tolerate camelCase for copy-pasted configs.
        value = _dig(view, [section, leaf])
        if value is None and section == "metadata":
            value = _dig(view, [section, _camel(leaf)])
        if value is None:
            break  # view exists in YAML but the key doesn't — try common_views.
        if isinstance(value, list):
            value = description_key_to_string([str(v) for v in value])
        return EditCandidate(
            file=_rel(config_path),
            yaml_path=f"views[{i}].{section}.{leaf}",
            kind="mdim-view",
            template=str(value),
        )

    # 2. definitions.common_views entries, most specific matching entry first.
    common_views = (config.get("definitions") or {}).get("common_views") or []
    matching = [
        (j, entry)
        for j, entry in enumerate(common_views)
        if _common_view_matches(entry.get("dimensions") or {}, view_dims)
    ]
    matching.sort(key=lambda item: -len(item[1].get("dimensions") or {}))
    for j, entry in matching:
        value = _dig(entry, [section, leaf])
        if value is None:
            continue
        if isinstance(value, list):
            value = description_key_to_string([str(v) for v in value])
        affected = f"applies to every view matching {entry.get('dimensions') or 'ALL views'}"
        return EditCandidate(
            file=_rel(config_path),
            yaml_path=f"definitions.common_views[{j}].{section}.{leaf}",
            kind="mdim-common-views",
            template=str(value),
            notes=[f"Shared `common_views` entry — editing it {affected}."],
        )
    return None


def _common_view_matches(entry_dims: dict[str, Any], view_dims: dict[str, str]) -> bool:
    """A common_views entry applies when all its dimension constraints match the view."""
    for key, value in entry_dims.items():
        if view_dims.get(underscore(str(key), validate=False)) != underscore(str(value), validate=False):
            return False
    return True


def _camel(snake: str) -> str:
    parts = snake.split("_")
    return parts[0] + "".join(p.title() for p in parts[1:])

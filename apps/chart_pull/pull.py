"""Pure logic behind `etl chart-pull`: fold a chart's admin overrides into its ETL config.

An ETL-authored single chart renders as merge(indicator config, ETL layer, admin layer). Admin edits
land in the admin layer (`charts.patchConfigId`) and win over ETL, which the admin flags as drift:
any top-level key in the admin patch besides the identity keys every patch carries
(`KEYS_EXCLUDED_FROM_INHERITANCE`). The fix is writing those values into the chart's `.config.yml`.
Grapher re-diffs the admin patch against the new ETL layer on every push, so once a value is in the
YAML, it drops out of the admin layer by itself.

Nothing here touches a database; the CLI fetches the layers and passes them in.
"""

import copy
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.scalarstring import DoubleQuotedScalarString

from apps.chart_clean.rules import find_noop_fields, remove_paths
from etl.dag_helpers import load_single_dag_file
from etl.files import ruamel_dump, ruamel_load

# Keys every admin patch carries regardless of drift (grapherConfigInheritance.ts in owid-grapher).
KEYS_EXCLUDED_FROM_INHERITANCE = ("$schema", "id", "slug", "version", "isPublished")
# Grapher config `dimensions` properties, in the order ETL emits them (etl/viz/chart/upsert.py).
AXES = ("y", "x", "size", "color")
# Same text scan as tests/test_chart_config_ids.py: config files may carry Jinja.
CHART_CONFIG_ID_RE = re.compile(r"^\s*chart_config_id:\s*[\"']?([0-9a-fA-F-]{36})", re.MULTILINE)
# Variable-id fields of explorer and multidim views. Grapher chart configs don't define them (the
# schema has `additionalProperties: false`), so a single chart's YAML can't carry them.
VARIABLE_ID_FIELDS = ("colorVariableId", "xVariableId", "sizeVariableId")
CONFIG_SUFFIX = ".config.yml"


@dataclass
class ChartStep:
    """An ETL-authored single chart: its config file and DAG entry."""

    config_path: Path
    chart_config_id: str
    namespace: str
    version: str
    short_name: str
    dag_file: Path | None = None
    dependencies: set[str] = field(default_factory=set)

    @property
    def uri(self) -> str:
        return f"viz://chart/{self.namespace}/{self.version}/{self.short_name}"

    @property
    def has_code(self) -> bool:
        return self.config_path.with_name(f"{self.short_name}.py").exists()


@dataclass
class ChartLayers:
    """What the source environment holds for one chart."""

    chart_id: int
    patch: dict[str, Any]
    # The rendered config, which tells which admin fields have no effect (apps/chart_clean/rules.py).
    full: dict[str, Any] = field(default_factory=dict)
    # variableId -> catalogPath, for every variable id the patch references.
    variable_paths: dict[int, str | None] = field(default_factory=dict)
    # Whether a person (not ETL) saved the chart in the admin after its ETL layer was created.
    # False means every admin override predates the move to ETL; None means unknown.
    edited_after_adoption: bool | None = None


@dataclass
class Result:
    """Outcome of pulling one chart."""

    step: ChartStep
    chart_id: int | None = None
    admin_url: str | None = None
    pulled: list[str] = field(default_factory=list)
    # Pulled fields that replace a value the YAML already set, rather than adding one.
    overrides_etl: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    # Admin fields with no effect on the chart: left out, to clear in the admin with `etl chart-clean`.
    noop: list[str] = field(default_factory=list)
    # Admin values set before the chart moved to ETL that would replace a newer ETL value: left out.
    leftovers: list[str] = field(default_factory=list)
    # Admin pins to an older version of an indicator the YAML already names: no push clears them.
    stale_pins: list[str] = field(default_factory=list)
    already_in_etl: list[str] = field(default_factory=list)
    needs_manual: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    missing: bool = False

    @property
    def status(self) -> str:
        if self.missing:
            return "not found"
        labels = []
        if self.pulled:
            labels.append("fields pulled")
        if self.skipped:
            labels.append("fields skipped")
        if self.noop:
            labels.append("no-op fields")
        if self.leftovers:
            labels.append("admin leftovers")
        if self.stale_pins:
            labels.append("stale variable pins")
        if self.already_in_etl:
            labels.append("already in ETL")
        if self.needs_manual:
            labels.append("needs manual")
        return ", ".join(labels) or "in sync"

    def to_dict(self) -> dict[str, Any]:
        return {
            "chart_id": self.chart_id,
            "admin_url": self.admin_url,
            "step": self.step.uri,
            "config_path": str(self.step.config_path),
            "status": self.status,
            "fields_pulled": self.pulled,
            "fields_overriding_etl": self.overrides_etl,
            "fields_skipped": self.skipped,
            "fields_noop": self.noop,
            "fields_admin_leftovers": self.leftovers,
            "fields_stale_variable_pins": self.stale_pins,
            "fields_already_in_etl": self.already_in_etl,
            "needs_manual": self.needs_manual,
            "warnings": self.warnings,
        }


def find_chart_steps(step_dir: Path, dag_dir: Path) -> list[ChartStep]:
    """Every ETL-authored single chart in the repo, with its DAG file and dependencies."""
    dag_entries: dict[str, tuple[Path, set[str]]] = {}
    for dag_file in sorted(dag_dir.glob("*.yml")):
        for step, deps in load_single_dag_file(dag_file).items():
            if step.startswith("viz://chart/"):
                dag_entries[step] = (dag_file, set(deps))

    steps = []
    for path in sorted((step_dir / "viz" / "chart").glob(f"*/*/*{CONFIG_SUFFIX}")):
        match = CHART_CONFIG_ID_RE.search(path.read_text())
        if not match:
            continue
        step = ChartStep(
            config_path=path,
            chart_config_id=match.group(1).lower(),
            namespace=path.parent.parent.name,
            version=path.parent.name,
            short_name=path.name.removesuffix(CONFIG_SUFFIX),
        )
        if step.uri in dag_entries:
            step.dag_file, step.dependencies = dag_entries[step.uri]
        steps.append(step)
    return steps


def depends_on_dataset(step: ChartStep, dataset: str) -> bool:
    """Whether the chart's step depends on a grapher dataset.

    `dataset` is `ns/version/short`, `ns/short` (any version), or a full `data://grapher/...` URI.
    """
    dataset = re.sub(r"^(data|data-private)://grapher/", "", dataset)
    parts = dataset.split("/")
    if len(parts) == 3:
        pattern = re.escape(dataset)
    elif len(parts) == 2:
        pattern = rf"{re.escape(parts[0])}/[^/]+/{re.escape(parts[1])}"
    else:
        raise ValueError(f"Dataset must look like `namespace/version/short_name` or `namespace/short_name`: {dataset}")
    regex = re.compile(rf"^(data|data-private)://grapher/{pattern}$")
    return any(regex.match(dep) for dep in step.dependencies)


def field_overrides(patch: dict[str, Any]) -> dict[str, Any]:
    """The admin patch minus the identity keys: what the admin's ETL badge counts as drift."""
    return {k: v for k, v in patch.items() if k not in KEYS_EXCLUDED_FROM_INHERITANCE}


def variable_ids_in_patch(patch: dict[str, Any]) -> set[int]:
    """Variable ids the patch references, which the CLI resolves to catalog paths."""
    ids = {int(d["variableId"]) for d in patch.get("dimensions", []) if "variableId" in d}
    for value in (patch.get("sortColumnSlug"), (patch.get("map") or {}).get("columnSlug")):
        variable_id = _as_variable_id(value)
        if variable_id is not None:
            ids.add(variable_id)
    return ids


def pull_fields(
    step: ChartStep,
    config: CommentedMap,
    layers: ChartLayers,
    result: Result,
    skip_fields: tuple[str, ...] = (),
    pull_leftovers: bool = False,
) -> None:
    """Fold the admin patch's field overrides into the config's single view, in place.

    Pulling never changes what readers see, since the admin value is what renders today. A pulled
    field that replaces a value the YAML already set is also listed in `result.overrides_etl`.

    When moving a chart into ETL, its whole admin config stays in the admin layer, and only values
    equal to ETL's drop out on push. So if no person has saved the chart since its ETL layer was
    created, an admin value that differs from the YAML's is a leftover from before the move, masking
    the newer ETL value. Those are left out (`result.leftovers`) unless `pull_leftovers` is set.

    Admin fields that have no effect on the chart (`etl chart-clean`'s rules) are left out too
    (`result.noop`): they belong cleared in the admin, not copied into ETL.
    """
    view = config["views"][0]
    if layers.full:
        result.noop = [f.path for f in find_noop_fields(layers.full, layers.patch)]
    overrides = field_overrides(remove_paths(layers.patch, result.noop))
    resolver = _PathResolver(view, layers.variable_paths, step.dependencies)
    leftovers_possible = layers.edited_after_adoption is False and not pull_leftovers

    for key, value in overrides.items():
        if key in skip_fields:
            result.skipped.append(key)
            continue
        if key == "dimensions":
            indicators, problems = _indicators_from_dimensions(value, resolver)
            if problems:
                result.needs_manual.extend(problems)
                continue
            stale = any(int(d["variableId"]) in resolver.stale_ids for d in value)
            existing = _canonical_indicators(view.get("indicators"))
            if existing == _canonical_indicators(indicators):
                # A pure stale pin writes nothing: the YAML already names the current versions.
                (result.stale_pins if stale else result.already_in_etl).append(key)
                continue
            # Other admin edits (order, display, indicators) are handled as usual, but the admin layer
            # keeps the old ids until it is cleared, so the pin is still reported.
            if stale:
                result.stale_pins.append(key)
            if existing and leftovers_possible:
                result.leftovers.append(key)
            else:
                view["indicators"] = indicators
                result.pulled.append(key)
                if existing:
                    result.overrides_etl.append(key)
            continue

        if key in VARIABLE_ID_FIELDS:
            result.needs_manual.append(
                f"{key}: not a grapher chart config field, so the chart's YAML can't carry it; remove it in the admin"
            )
            continue

        # Indicator references in the patch are variable ids; the YAML names indicators by path.
        value = copy.deepcopy(value)
        problems = []
        if key == "sortColumnSlug":
            value, problems = _resolve_column_slug(value, resolver, key)
        elif key == "map" and isinstance(value, dict) and "columnSlug" in value:
            value["columnSlug"], problems = _resolve_column_slug(value["columnSlug"], resolver, "map.columnSlug")
        if problems:
            result.needs_manual.extend(problems)
            continue
        # A pin to an older version of an indicator the YAML names. The admin keeps the old id until it
        # is cleared, so it is reported either way; the current path is copied when the YAML doesn't
        # set this field yet, so that clearing the pin keeps the chart's sort or map column.
        yaml_config = view.get("config") or {}
        if key == "sortColumnSlug" and _pins_stale_id(layers.patch.get(key), resolver):
            result.stale_pins.append(key)
            if yaml_config.get(key) is not None:
                continue
        if key == "map" and "columnSlug" in value and _pins_stale_id(layers.patch["map"].get("columnSlug"), resolver):
            result.stale_pins.append("map.columnSlug")
            if (yaml_config.get("map") or {}).get("columnSlug") is not None:
                del value["columnSlug"]
                if not _plain(value):
                    continue

        if view.get("config") is None:
            view["config"] = CommentedMap()
        before = _plain(view["config"].get(key))
        if leftovers_possible and _replaces_existing(before, _plain(value)):
            # Only the leaves that replace an ETL value are leftovers; keys ETL doesn't set are still copied.
            value, leftover_paths = _split_leftovers(before, _plain(value), key)
            result.leftovers.extend(leftover_paths)
            if value is None:
                continue
        _merge_into(view["config"], key, value)
        if _plain(view["config"].get(key)) == before:
            result.already_in_etl.append(key)
        else:
            result.pulled.append(key)
            if _replaces_existing(before, _plain(value)):
                result.overrides_etl.append(key)

    if step.has_code and result.pulled:
        result.warnings.append(
            f"{step.short_name}.py may recompute some of the pulled fields from the data: check them before committing."
        )
    patch_schema = _schema_number(layers.patch.get("$schema"))
    yaml_schema = _schema_number(config.get("grapher_schema"))
    if patch_schema and yaml_schema and patch_schema > yaml_schema:
        result.warnings.append(
            f"The admin patch uses grapher schema {patch_schema}, newer than the config's `grapher_schema: {yaml_schema}`."
        )


def load_config(path: Path) -> CommentedMap:
    """Load a chart config in round-trip mode (comments and key order survive)."""
    return cast(CommentedMap, ruamel_load(path))  # ty: ignore[invalid-argument-type]


def dump_config(config: CommentedMap) -> str:
    return ruamel_dump(config)


class _PathResolver:
    """Map a grapher variable id back to the catalog path the chart's YAML uses for it."""

    def __init__(self, view: CommentedMap, variable_paths: dict[int, str | None], dependencies: set[str]):
        self.variable_paths = variable_paths
        self.dependencies = dependencies
        self.yaml_paths = _yaml_paths(view)
        # Ids that match a YAML indicator only in another version of its dataset than the step
        # builds from: the admin still pins the old variable, which no ETL push can clear.
        self.stale_ids: set[int] = set()

    def resolve(self, variable_id: int) -> tuple[str | None, str | None]:
        """Return (path, problem)."""
        catalog_path = self.variable_paths.get(variable_id)
        if not catalog_path:
            return None, f"variable {variable_id} has no catalog path in grapher"
        # Prefer the path already written in the YAML, in whatever short form it uses. Paths are compared
        # ignoring the dataset version, which covers a step repointed to a new dataset version, where
        # the patch still holds old ids, but not a same-named table#column in an unrelated dataset.
        target = _unversioned(catalog_path)
        grapher_deps = [
            d.split("://", 1)[1] for d in self.dependencies if re.match(r"^(data|data-private)://grapher/", d)
        ]
        dataset_uri, _, column_part = catalog_path.rpartition("/")
        for path in self.yaml_paths:
            if catalog_path == path or target in _unversioned_candidates(path, grapher_deps):
                if dataset_uri not in grapher_deps and catalog_path != path:
                    self.stale_ids.add(variable_id)
                return path, None
        # Otherwise name it relative to the step's grapher dependencies.
        if dataset_uri in grapher_deps:
            if len(grapher_deps) == 1:
                return column_part, None
            return f"{dataset_uri.rpartition('/')[2]}/{column_part}", None
        return None, (
            f"variable {variable_id} ({catalog_path}) comes from a dataset the step doesn't depend on; "
            "add the dependency and the indicator by hand"
        )


def _yaml_paths(view: CommentedMap) -> list[str]:
    paths = []
    indicators = view.get("indicators") or {}
    for axis in AXES:
        entries = indicators.get(axis)
        if entries is None:
            continue
        for entry in entries if isinstance(entries, list) else [entries]:
            paths.append(entry if isinstance(entry, str) else entry["catalogPath"])
    config = view.get("config") or {}
    for value in (config.get("sortColumnSlug"), (config.get("map") or {}).get("columnSlug")):
        if isinstance(value, str) and "#" in value:
            paths.append(value)
    return paths


def _pins_stale_id(value: Any, resolver: _PathResolver) -> bool:
    return isinstance(value, str) and value.isdigit() and int(value) in resolver.stale_ids


def _unversioned(path: str) -> str:
    """Drop the version from a full catalog path: `grapher/ns/version/dataset/table#column` -> `grapher/ns/dataset/table#column`."""
    parts = path.split("/", 4)
    if len(parts) == 5:
        del parts[2]
    return "/".join(parts)


def _unversioned_candidates(yaml_path: str, dataset_uris: list[str]) -> set[str]:
    """Full catalog paths (without version) that a YAML indicator path can stand for, given the step's grapher deps."""
    if yaml_path.count("/") >= 4:
        return {_unversioned(yaml_path)}
    if "/" in yaml_path:
        dataset_name, _, column_part = yaml_path.partition("/")
        uris = [uri for uri in dataset_uris if uri.rpartition("/")[2] == dataset_name]
    else:
        column_part, uris = yaml_path, dataset_uris
    return {_unversioned(f"{uri}/{column_part}") for uri in uris}


def _indicators_from_dimensions(dimensions: list[dict[str, Any]], resolver: _PathResolver):
    """Rebuild `indicators` from grapher `dimensions`, keeping their order and display."""
    by_axis: dict[str, list[Any]] = {}
    problems = []
    # A single chart needs at least one indicator, or its next push fails.
    if not dimensions:
        problems.append("dimensions: the admin sets no indicators, and a single chart needs at least one")
    for dim in dimensions:
        extra = set(dim) - {"property", "variableId", "display"}
        if extra:
            problems.append(
                f"dimension of variable {dim.get('variableId')} sets {sorted(extra)}, which ETL can't express"
            )
            continue
        path, problem = resolver.resolve(int(dim["variableId"]))
        if problem:
            problems.append(f"dimensions: {problem}")
            continue
        entry: Any = DoubleQuotedScalarString(path)
        if dim.get("display"):
            entry = CommentedMap([("catalogPath", entry), ("display", _yaml_value(dim["display"]))])
        by_axis.setdefault(dim["property"], []).append(entry)

    unknown = set(by_axis) - set(AXES)
    if unknown:
        problems.append(f"dimensions use properties {sorted(unknown)}, which ETL can't express")
    for axis in AXES[1:]:
        if len(by_axis.get(axis, [])) > 1:
            problems.append(f"dimensions have {len(by_axis[axis])} `{axis}` indicators; ETL allows one")

    indicators = CommentedMap()
    for axis in AXES:
        if axis in by_axis:
            indicators[axis] = CommentedSeq(by_axis[axis]) if axis == "y" else by_axis[axis][0]
    return indicators, problems


def _canonical_indicators(indicators: Any) -> dict[str, list[dict[str, Any]]]:
    """Indicators in one shape, so `"t#c"` and `{catalogPath: "t#c"}` compare equal."""
    canonical = {}
    for axis in AXES:
        entries = (indicators or {}).get(axis)
        if entries is None:
            continue
        canonical[axis] = [
            {"catalogPath": e, "display": None}
            if isinstance(e, str)
            else {"catalogPath": e["catalogPath"], "display": _plain(e.get("display")) or None}
            for e in (entries if isinstance(entries, list) else [entries])
        ]
    return canonical


def _as_variable_id(value: Any) -> int | None:
    """A variable id written as an int or a digit string, else None."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _resolve_column_slug(value: Any, resolver: _PathResolver, label: str) -> tuple[Any, list[str]]:
    """Swap a variable id (column slug or `*VariableId` field) for the catalog path the YAML uses."""
    variable_id = _as_variable_id(value)
    if variable_id is not None:
        path, problem = resolver.resolve(variable_id)
        if problem:
            return value, [f"{label}: {problem}"]
        return path, []
    return value, []


def _merge_into(target: CommentedMap, key: str, value: Any) -> None:
    """Grapher's merge for one key: objects merge recursively, arrays and scalars replace."""
    if isinstance(value, dict) and isinstance(target.get(key), dict):
        for sub_key, sub_value in value.items():
            _merge_into(target[key], sub_key, sub_value)
    else:
        target[key] = _yaml_value(value)


def _replaces_existing(before: Any, value: Any) -> bool:
    """Whether merging `value` changes a leaf the YAML already set (rather than only adding keys)."""
    if before is None:
        return False
    if isinstance(before, dict) and isinstance(value, dict):
        return any(_replaces_existing(before.get(k), v) for k, v in value.items())
    return before != value


def _split_leftovers(before: Any, value: Any, path: str) -> tuple[Any, list[str]]:
    """Split an override into what replaces nothing in the YAML and the dotted paths of the leaves it replaces.

    Returns (value to merge or None, leftover paths), e.g. `map.colorScale` when `map.columnSlug` is admin-only.
    """
    if not (isinstance(before, dict) and isinstance(value, dict)):
        return None, [path]
    kept, leftovers = {}, []
    for key, sub_value in value.items():
        if _replaces_existing(before.get(key), sub_value):
            sub_kept, sub_leftovers = _split_leftovers(before.get(key), sub_value, f"{path}.{key}")
            leftovers += sub_leftovers
            if sub_kept is not None:
                kept[key] = sub_kept
        else:
            kept[key] = sub_value
    return (kept or None), leftovers


def _yaml_value(value: Any) -> Any:
    """Turn a JSON value into YAML nodes styled like the configs (double-quoted strings)."""
    if isinstance(value, dict):
        return CommentedMap((k, _yaml_value(v)) for k, v in value.items())
    if isinstance(value, list):
        return CommentedSeq(_yaml_value(v) for v in value)
    if isinstance(value, str):
        return DoubleQuotedScalarString(value)
    return value


def _plain(value: Any) -> Any:
    """Comparable plain-JSON form of a YAML node."""
    return json.loads(json.dumps(value, default=str))


def _schema_number(value: Any) -> int | None:
    match = re.search(r"(\d{3})", str(value or ""))
    return int(match.group(1)) if match else None

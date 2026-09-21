import copy
from typing import Any

from jsonschema import Draft202012Validator, validate, validators
from structlog import get_logger

from etl.files import get_schema_from_url

# Logger
log = get_logger()


def validate_chart_config(config: dict[str, Any]) -> None:
    """Validate the schema of a chart configuration."""
    schema = get_schema_from_url(config["$schema"])
    validate(config, schema)


def validate_chart_config_and_set_defaults(
    config: dict[str, Any], schema: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Add properties with default values to a config file, if they are not present.

    Parameters
    ----------
    config : Dict[str, Any]
        JSON-like object. Typically the chart configuration field.
    schema: Dict[str, Any]
        JSON-like object. Schema of the chart configuration. If none is provided, the latest schema is downloaded.

    Returns
    -------
    Dict[str, Any]
        Updated object, with defaults set.
    """
    log.info("schema: validating schema and adding defaults")

    def _extend_with_set_default(validator_class):  # ty: ignore
        validate_properties = validator_class.VALIDATORS["properties"]

        def _set_defaults(validator, properties, instance, schema):  # ty: ignore
            for property, subschema in properties.items():
                if "default" in subschema:
                    instance.setdefault(property, subschema["default"])

            yield from validate_properties(
                validator,
                properties,
                instance,
                schema,
            )

        return validators.extend(
            validator_class,
            {"properties": _set_defaults},
        )

    # Create custom validation object
    DefaultSetterValidatingValidator = _extend_with_set_default(Draft202012Validator)
    # Get schema
    if schema is None:
        schema = get_schema_from_url(config["$schema"])

    # Validate and update config with defaults
    config_new = copy.deepcopy(config)
    # Remove chart table fields if present as they're not part of the schema
    if "isInheritanceEnabled" in config_new:
        del config_new["isInheritanceEnabled"]
    if "forceDatapage" in config_new:
        del config_new["forceDatapage"]
    # Remove adminBaseUrl and bakedGrapherURL if present
    if "adminBaseUrl" in config_new:
        del config_new["adminBaseUrl"]
    if "bakedGrapherURL" in config_new:
        del config_new["bakedGrapherURL"]

    try:
        DefaultSetterValidatingValidator(schema).validate(config_new)
    except Exception as e:
        raise Exception(f"Could not validate schema for chart {config['id']}: {e}")

    # NOTE: I think we do not need the code below. Uncomment if encountering issues with the schema.
    # Add minTime if not set (no default provided in schema)
    # Kinda hacky
    # if len(config_new["chartTypes"]) > 0:
    #     if config_new["chartTypes"][0] not in {"StackedDiscreteBar", "Marimekko", "DiscreteBar"}:
    #         if "minTime" not in config_new:
    #             config_new["minTime"] = "earliest"
    return config_new


def fix_errors_in_schema(config: dict[str, Any]) -> dict[str, Any]:
    """Fix common errors in schema and tries to catch up with latest schema version."""
    config_new = copy.deepcopy(config)
    if "map" in config_new:
        assert "variableId" not in config_new["map"], "map.variableId has been deprecated by map.columnSlug"
    if ("timelineMaxTime" in config_new) and (config_new["timelineMaxTime"] is None):
        del config_new["timelineMaxTime"]
    if ("timelineMinTime" in config_new) and (config_new["timelineMinTime"] is None):
        del config_new["timelineMinTime"]
    return config_new


def validate_chart_config_and_remove_defaults(
    config: dict[str, Any], schema: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Remove properties with values equal to their defaults from schema.

    Parameters
    ----------
    config : Dict[str, Any]
        JSON-like object. Typically the chart configuration field.
    schema: Dict[str, Any]
        JSON-like object. Schema of the chart configuration. If none is provided, the latest schema is downloaded.

    Returns
    -------
    Dict[str, Any]
        Updated object, with defaults set.
    """
    log.info("schema: validating schema and removing defaults")

    def _extend_with_remove_default(validator_class):  # ty: ignore
        validate_properties = validator_class.VALIDATORS["properties"]

        def _set_defaults(validator, properties, instance, schema):  # ty: ignore
            for property, subschema in properties.items():
                is_required = property in (schema or {}).get("required", [])
                if "default" in subschema:
                    if not is_required and subschema["default"] == instance[property]:
                        del instance[property]

            yield from validate_properties(
                validator,
                properties,
                instance,
                schema,
            )

        return validators.extend(
            validator_class,
            {"properties": _set_defaults},
        )

    # Create custom validation object
    DefaultDeleteValidatingValidator = _extend_with_remove_default(Draft202012Validator)
    # Get schema
    if schema is None:
        schema = get_schema_from_url(config["$schema"])
    # Validate and update config with defaults
    config_new = copy.deepcopy(config)
    DefaultDeleteValidatingValidator(schema).validate(config_new)
    return config_new


_MISSING = object()


def _collect_explicit_leaves(config: Any) -> Any:
    """Mirror `config`'s shape, replacing every leaf (or list) with `True`.

    Used to remember which paths were explicitly present *before* schema defaults were
    filled in, so a later pruning step can tell "the chart genuinely set this" apart from
    "this only exists because we filled it in with the generic schema default."
    """
    if isinstance(config, dict):
        return {key: _collect_explicit_leaves(value) for key, value in config.items()}
    return True


def _prune_to_explicit(value: Any, explicit: Any, baseline: Any) -> Any:
    """Keep only the parts of `value` that were explicitly set (per `explicit`) and that
    differ from `baseline` (what the indicator's own config would provide via inheritance).

    Returns `_MISSING` when nothing should be kept at this level.
    """
    if isinstance(value, dict) and isinstance(explicit, dict):
        result = {}
        for key, sub_value in value.items():
            sub_explicit = explicit.get(key, _MISSING)
            if sub_explicit is _MISSING:
                continue  # never explicitly set anywhere in this subtree -> drop
            sub_baseline = baseline.get(key, _MISSING) if isinstance(baseline, dict) else _MISSING
            pruned = _prune_to_explicit(sub_value, sub_explicit, sub_baseline)
            if pruned is not _MISSING:
                result[key] = pruned
        return result if result else _MISSING
    # Leaf (or a list, e.g. `dimensions`): keep only if it was explicitly set and its
    # final value differs from what inheriting from the indicator would already give.
    if explicit and value != baseline:
        return value
    return _MISSING


def compute_inheritance_patch(
    config: dict[str, Any],
    indicator_config: dict[str, Any],
    schema: dict[str, Any] | None = None,
    original_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Strip parts of `config` that resolve to the same value as the indicator's own ETL
    config would already provide via inheritance.

    `validate_chart_config_and_remove_defaults` strips any field matching the generic
    JSON-schema default, which silently reverts genuine overrides that happen to match
    that default but differ from what the chart's indicator itself sets (e.g. a chart
    disables hasMapTab even though the indicator enables it, and the schema default for
    hasMapTab also happens to be false) -- see #5911. Simply keeping every field whenever it
    differs from the *schema* default overcorrects, though: a field the chart never touched
    still got filled in by `validate_chart_config_and_set_defaults` upstream, so comparing
    it against the indicator's config would (correctly) find them equal and keep it out --
    but if the chart didn't set it and the indicator did (e.g. indicator sets hasMapTab=true,
    chart is silent), comparing schema-filled values would wrongly treat the schema default
    as the chart's "explicit" value and produce a spurious override. So we track which paths
    were *actually* present in the original, pre-defaults chart config (via
    `original_config`, defaulting to `config` when the caller already has the raw config) and
    only ever consider those for inclusion -- a field is kept exactly when the chart
    explicitly set it to something that resolves differently than plain inheritance would.

    `indicator_config` is filled with schema defaults so it reflects the same effective
    values inheritance would actually provide.
    """
    if schema is None:
        schema = get_schema_from_url(config["$schema"])
    if original_config is None:
        original_config = config

    # `indicator_config` is `{}` when the indicator has no `grapherConfigETL` at all (an
    # explicit "inherits nothing" baseline, see `_fetch_single_indicator_config`) -- but the
    # schema's own top-level `required` fields (e.g. `$schema`, `dimensions`) have no
    # "default" to fall back on, so validating a bare `{}` against it fails and crashes here.
    # Borrow those required fields from `config` (which is already a valid, fully-formed
    # chart config) purely so validation succeeds; they don't affect the comparison below,
    # since `_prune_to_explicit` only ever looks at paths present in `explicit`.
    baseline_input = {**indicator_config}
    for field in schema.get("required", []):
        if field not in baseline_input and field in config:
            baseline_input[field] = config[field]
    baseline_full = validate_chart_config_and_set_defaults(baseline_input, schema)
    explicit = _collect_explicit_leaves(original_config)

    pruned = _prune_to_explicit(config, explicit, baseline_full)
    result: dict[str, Any] = pruned if isinstance(pruned, dict) else {}

    # Always keep schema-required top-level fields, even when they happen to match the
    # indicator's own value (e.g. $schema), matching validate_chart_config_and_remove_defaults's
    # own is_required guard.
    for field in schema.get("required", []):
        if field not in result and field in config:
            result[field] = config[field]

    return result


def prune_dimension_displays(
    config: dict[str, Any],
    display_baselines: dict[int, dict[str, Any]],
    original_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Strip `dimensions[*].display` fields that resolve to the same value as the
    corresponding variable's own `display` metadata.

    This is a *separate* inheritance path from `compute_inheritance_patch`'s
    grapherConfigETL-based one: a chart dimension's `display` object (tolerance,
    numDecimalPlaces, unit, ...) inherits from that indicator's own `variables.display`
    column, not from its `grapherConfigETL`. Left unhandled, this pocket of bloat
    (`zeroDay`, `isProjection`, `roundingMode`, `numSignificantFigures`, ...) survives
    `compute_inheritance_patch` untouched, since indicator configs have no per-dimension
    `display` to compare against.

    `display_baselines` maps each dimension's (already-remapped) variable ID to that
    variable's `display` metadata. Dimensions are matched to `original_config` by
    position, since their `variableId` itself changes across the remap.
    """
    if original_config is None:
        original_config = config

    config = copy.deepcopy(config)
    original_dimensions = original_config.get("dimensions", [])

    for i, dimension in enumerate(config.get("dimensions", [])):
        if "display" not in dimension:
            continue
        baseline_display = display_baselines.get(dimension.get("variableId"))
        if baseline_display is None:
            continue
        original_dimension = original_dimensions[i] if i < len(original_dimensions) else {}
        explicit_display = _collect_explicit_leaves(original_dimension.get("display", {}))
        pruned_display = _prune_to_explicit(dimension["display"], explicit_display, baseline_display)
        if isinstance(pruned_display, dict) and pruned_display:
            dimension["display"] = pruned_display
        else:
            del dimension["display"]

    return config

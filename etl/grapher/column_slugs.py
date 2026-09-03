"""Repairing `map.columnSlug` / `sortColumnSlug` references that name no column the chart plots.

Both fields pick one of a chart's columns — which indicator the map tab colours, which one a
bar chart sorts by — and both identify it by variable id stored as a *string* (see the note in
`etl.grapher.model._remap_variable_ids`; it wasn't a great decision). Nothing in Grapher ties
either field to `dimensions`, so a slug goes **dangling** the moment the dimensions change
without it: the chart plots one variable and the slug names another.

Grapher tolerates that — the map falls back to the first y column — which is exactly why it
goes unnoticed. Two things make it worth repairing rather than leaving:

- On a chart with several y columns the fallback is a *different indicator* than the one the
  author picked, and nothing says so.
- `_remap_variable_ids` (chart-sync) drops a dangling slug outright on the way to production,
  so the author's choice is not just overridden but deleted.

A dangling slug almost always means the same indicator at a new version, so the repair is a
remap, not a delete: match the retired variable to a plotted one on catalog path ignoring the
version segment. Deleting is the fallback for when no plotted column is that indicator.

The functions here are pure — callers resolve variable ids to catalog paths themselves and
pass them in — so the matching rules can be tested without a database.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

# The two config fields that name a column by variable id. `map.columnSlug` is nested, so each
# entry carries a reader and a writer rather than a bare key.
MAP_COLUMN_SLUG = "map.columnSlug"
SORT_COLUMN_SLUG = "sortColumnSlug"
COLUMN_SLUG_FIELDS = (MAP_COLUMN_SLUG, SORT_COLUMN_SLUG)


@dataclass(frozen=True)
class SlugRepair:
    """One field that named a column the chart doesn't plot, and what became of it."""

    field: str
    old_id: int
    # None means the field was removed: no plotted column is that indicator.
    new_id: int | None
    reason: str

    def __str__(self) -> str:
        target = f"-> {self.new_id}" if self.new_id is not None else "dropped"
        return f"{self.field}: {self.old_id} {target} ({self.reason})"


def indicator_identity(catalog_path: str | None) -> str | None:
    """Strip the version from a grapher catalog path, leaving what is stable across versions.

    `grapher/animal_welfare/2026-04-16/chick_culling_laws/chick_culling_laws#status`
    becomes `grapher/animal_welfare/chick_culling_laws/chick_culling_laws#status`, so the same
    indicator re-published under a new version matches.

    Returns None when there is nothing to match on: legacy variables carry no catalog path at
    all, and a path that isn't the usual five segments (`channel/namespace/version/dataset/
    table#column`) is not one we can take a version out of. Every catalog path in grapher has
    five segments today; returning None keeps an unexpected shape from being mangled into a
    false match.
    """
    if not catalog_path:
        return None
    parts = catalog_path.split("/")
    if len(parts) != 5:
        return None
    return "/".join(parts[:2] + parts[3:])


def dimension_variable_ids(config: Mapping[str, Any]) -> list[int]:
    """Variable ids the chart plots, in `dimensions` order."""
    ids = []
    for dimension in config.get("dimensions") or []:
        variable_id = dimension.get("variableId")
        if variable_id is not None:
            ids.append(int(variable_id))
    return ids


def read_column_slug(config: Mapping[str, Any], field: str) -> int | None:
    """The variable id a column-slug field names, or None if it's absent or not an id.

    A collection config may declare either field as a catalog path (`etl.collection.model.view`
    expands it and `chart_upsert` resolves it to an id before pushing), so a non-numeric value
    here is authored input that hasn't been resolved yet — not something to repair.
    """
    if field == MAP_COLUMN_SLUG:
        map_config = config.get("map")
        value = map_config.get("columnSlug") if isinstance(map_config, dict) else None
    else:
        value = config.get(field)

    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def find_dangling_column_slugs(config: Mapping[str, Any]) -> dict[str, int]:
    """Which column-slug fields name a variable the chart doesn't plot, and the id each names."""
    plotted = set(dimension_variable_ids(config))
    dangling = {}
    for field in COLUMN_SLUG_FIELDS:
        slug_id = read_column_slug(config, field)
        if slug_id is not None and slug_id not in plotted:
            dangling[field] = slug_id
    return dangling


def variable_ids_to_resolve(config: Mapping[str, Any]) -> set[int]:
    """Every variable id `repair_column_slugs` needs a catalog path for."""
    return set(dimension_variable_ids(config)) | set(find_dangling_column_slugs(config).values())


def repair_column_slugs(
    config: Mapping[str, Any],
    catalog_paths: Mapping[int, str | None],
) -> tuple[dict[str, Any], list[SlugRepair]]:
    """Point every dangling column-slug field at a column the chart plots, or remove it.

    `catalog_paths` maps variable id to `variables.catalogPath` for the ids in
    `variable_ids_to_resolve` — a retired variable that has since been deleted simply won't be
    a key, which reads the same as having no catalog path: nothing to match on.

    Returns a new config (the input is left alone) and one `SlugRepair` per field changed. An
    empty repair list means the config already named plotted columns, and callers should write
    nothing.
    """
    dangling = find_dangling_column_slugs(config)
    if not dangling:
        return dict(config), []

    # Plotted columns by version-agnostic identity. An identity claimed by more than one
    # dimension (a chart deliberately plotting two versions of one indicator) is ambiguous, and
    # there is no basis for picking — drop those rather than guess.
    plotted_by_identity: dict[str, list[int]] = {}
    for variable_id in dimension_variable_ids(config):
        identity = indicator_identity(catalog_paths.get(variable_id))
        if identity is not None:
            plotted_by_identity.setdefault(identity, []).append(variable_id)

    repaired = dict(config)
    repairs = []
    for field, old_id in dangling.items():
        identity = indicator_identity(catalog_paths.get(old_id))
        candidates = plotted_by_identity.get(identity, []) if identity is not None else []

        if len(candidates) == 1:
            repair = SlugRepair(field, old_id, candidates[0], "same indicator, new version")
        elif identity is None:
            reason = (
                "retired variable no longer exists"
                if old_id not in catalog_paths
                else "no catalog path to match a plotted column on"
            )
            repair = SlugRepair(field, old_id, None, reason)
        elif candidates:
            repair = SlugRepair(field, old_id, None, f"ambiguous: {len(candidates)} plotted columns match")
        else:
            repair = SlugRepair(field, old_id, None, "chart no longer plots this indicator")

        _write_column_slug(repaired, field, repair.new_id)
        repairs.append(repair)

    return repaired, repairs


def _write_column_slug(config: dict[str, Any], field: str, new_id: int | None) -> None:
    """Set a column-slug field to `new_id`, or remove it when `new_id` is None."""
    if field == MAP_COLUMN_SLUG:
        # Copy before mutating: the caller's `map` dict is shared with the config we were given.
        map_config = dict(config["map"])
        if new_id is None:
            del map_config["columnSlug"]
        else:
            map_config["columnSlug"] = str(new_id)
        config["map"] = map_config
        return

    if new_id is not None:
        config[field] = str(new_id)
        return

    del config[field]
    # `sortBy: column` means "sort by sortColumnSlug", so leaving it behind claims a sort the
    # config can no longer describe — Grapher falls back at render time, but our own
    # indicator-upgrade path asserts the two travel together
    # (`etl.indicator_upgrade.indicator_update.update_chart_config_sort`). Drop it with the slug
    # and let the schema default (`total`) apply.
    if field == SORT_COLUMN_SLUG and config.get("sortBy") == "column":
        del config["sortBy"]

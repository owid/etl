"""Blast radius across surfaces: which *other* things use a changed indicator.

When an MDIM view's text changes because the underlying indicator metadata changed (not just
an MDIM override), that same change surfaces on every standalone chart and every other MDIM that
uses the indicator. This module answers "who else uses indicator X?" for those two surfaces.

Everything is keyed by the *staging* (source) variable id, and read-only. Explorers are being
phased out, so they are intentionally not covered here.
"""

import json
from typing import Any

from sqlalchemy.engine.base import Engine

from etl.db import read_sql


def _chunked(values: list, size: int = 500):
    for i in range(0, len(values), size):
        yield values[i : i + size]


def charts_using_indicators(engine: Engine, indicator_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    """indicator id -> published charts that use it (chartId, slug, title)."""
    result: dict[int, list[dict[str, Any]]] = {int(i): [] for i in indicator_ids}
    ids = [int(i) for i in set(indicator_ids)]
    if not ids:
        return result
    for chunk in _chunked(ids):
        placeholders = ", ".join(["%s"] * len(chunk))
        df = read_sql(
            f"""
            select cd.variableId as variableId,
                   c.id as chartId,
                   cc.slug as slug,
                   cc.full ->> '$.title' as title
            from chart_dimensions cd
            join charts c on c.id = cd.chartId
            join chart_configs cc on cc.id = c.configId
            where cd.variableId in ({placeholders})
              and cd.property = 'y'
              and c.publishedAt is not null
            """,
            engine=engine,
            params=tuple(chunk),
        )
        for record in df.to_dict("records"):
            result[int(record["variableId"])].append(
                {"chartId": int(record["chartId"]), "slug": record["slug"], "title": record["title"]}
            )
    # De-duplicate charts that reference the same indicator through several dimensions.
    for vid, charts in result.items():
        seen: dict[int, dict[str, Any]] = {}
        for c in charts:
            seen.setdefault(c["chartId"], c)
        result[vid] = list(seen.values())
    return result


def _indicator_ids_in_mdim_config(config: dict[str, Any]) -> set[int]:
    """Every indicator id referenced by any view of an MDIM config (y/x/color/size axes)."""
    ids: set[int] = set()
    for view in config.get("views") or []:
        indicators = view.get("indicators") or {}
        for axis in ("y", "x", "color", "size"):
            for item in indicators.get(axis) or []:
                if isinstance(item, dict) and item.get("id") is not None:
                    ids.add(int(item["id"]))
                elif isinstance(item, int):
                    ids.add(item)
    return ids


def mdims_using_indicators(
    engine: Engine, indicator_ids: list[int], exclude_catalog_path: str | None = None
) -> dict[int, list[dict[str, Any]]]:
    """indicator id -> *other* MDIMs that use it (catalogPath, slug).

    Indicator refs live inside each MDIM's JSON config, so we scan the configs in Python rather
    than in SQL — the config shape (`views[].indicators.y[]` as ids or objects) is more robustly
    handled here than with JSON path queries.
    """
    wanted = {int(i) for i in indicator_ids}
    result: dict[int, list[dict[str, Any]]] = {i: [] for i in wanted}
    if not wanted:
        return result
    df = read_sql(
        "select catalogPath, slug, config from multi_dim_data_pages where catalogPath is not null",
        engine=engine,
    )
    for record in df.to_dict("records"):
        if exclude_catalog_path is not None and record["catalogPath"] == exclude_catalog_path:
            continue
        raw = record["config"]
        try:
            config = json.loads(raw) if isinstance(raw, str) else raw
        except (TypeError, ValueError):
            continue
        if not isinstance(config, dict):
            continue
        for vid in _indicator_ids_in_mdim_config(config) & wanted:
            result[vid].append({"catalogPath": record["catalogPath"], "slug": record["slug"]})
    return result

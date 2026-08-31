"""Multi-dimensional data pages, whose views are charts in their own right.

An mdim is one slug with two or three dimensions — ``?metric=coverage&antigen=bcg`` — and every
combination is a chart a reader can land on. There are 40 published ones, and
``school-enrolment`` alone has 3 x 6 x 4 = 72 views.

Everything the critic needs already works per view: ``.png``, ``.metadata.json`` and ``.csv`` all
honour the dimension parameters and return that view's render, metadata and values. What is
missing without this module is *knowing the views exist* — reviewing an mdim's default view only
is like reviewing one country of a chart.

Two details worth knowing:

- Dimension parameter names are per-mdim (``metric``, ``antigen``, ``level``, ``sex`` …), so they
  cannot be hard-coded into a link allowlist. :func:`dimension_keys` supplies them.
- Choices whose slug ends in ``_side_by_side`` are comparison views rather than a single series.
  They are worth reviewing but are excluded from sampling by default, because a faceted view is
  harder to judge and the single-series views are where a bad number shows plainly.
"""

from __future__ import annotations

import json
from typing import Any

import numpy as np

SIDE_BY_SIDE = "_side_by_side"


def config(slug: str) -> dict[str, Any] | None:
    """The mdim config for a slug, or None if it is not an mdim (or there is no database)."""
    try:
        from etl.db import read_sql

        df = read_sql(
            "SELECT config FROM multi_dim_data_pages WHERE slug = %(slug)s AND published = 1 LIMIT 1",
            params={"slug": slug},
        )
    except Exception:  # noqa: BLE001 — no database just means mdim views are not enumerated
        return None
    if df.empty:
        return None
    raw = df.config.iloc[0]
    return json.loads(raw) if isinstance(raw, str) else raw


def dimension_keys(slug: str) -> set[str]:
    """The dimension parameter names for an mdim, e.g. ``{"metric", "antigen"}``.

    Needed so a finding's link keeps the parameters that select the view it is about.
    """
    cfg = config(slug)
    if not cfg:
        return set()
    return {d["slug"] for d in cfg.get("dimensions", []) if d.get("slug")}


def sample_views(slug: str, n: int, seed: int = 0, include_side_by_side: bool = False) -> list[tuple[str, str]]:
    """Up to ``n`` views of an mdim as ``(label, params)``, sampled from the views that exist.

    **Sample the config's own ``views`` list, never the cross product of the dimensions.** Not
    every combination is valid — ``electricity-mix`` declares 112 views out of 280 possible
    combinations — and requesting one that does not exist gets a ``500`` from the metadata and
    CSV endpoints, so a cross-product sampler fails on most of what it generates.

    Sampling beats taking the first N because the views are listed in a curated order: the first
    few are the ones an editor already looked at.
    """
    cfg = config(slug)
    if not cfg:
        return []
    views = [v.get("dimensions") for v in cfg.get("views", []) if v.get("dimensions")]
    if not include_side_by_side:
        views = [v for v in views if not any(str(x).endswith(SIDE_BY_SIDE) for x in v.values())] or views
    if not views:
        return []

    rng = np.random.default_rng(seed)
    idx = rng.choice(len(views), size=min(n, len(views)), replace=False)
    picked = []
    for i in idx:
        combo = views[int(i)]
        params = "&".join(f"{k}={v}" for k, v in combo.items())
        picked.append((", ".join(f"{k}={v}" for k, v in combo.items()), params))
    return picked

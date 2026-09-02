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
- A declared view is not guaranteed to render, so the render is best-effort and a review may be
  text-only. (A render-service bug on 2026-08-31 had ~82% of sampled mdim views answering 500;
  it was fixed the next day and 28/28 sampled views rendered. Kept as a note because the
  degradation path exists for a reason, not because mdims are inherently unrenderable.)
- Choices whose slug ends in ``_side_by_side`` are comparison views rather than a single series.
  They are worth reviewing but are excluded from sampling by default, because a faceted view is
  harder to judge and the single-series views are where a bad number shows plainly.
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlencode

import numpy as np

SIDE_BY_SIDE = "_side_by_side"


class MdimLookupError(Exception):
    """The mdim registry could not be read — as distinct from the slug not being an mdim.

    The difference matters: returning "not an mdim" when the database is simply unreachable makes
    ``--mdim-views N`` quietly review the bare slug once and report success, and makes ``--sample``
    treat every multi-dim page as an ordinary chart. Both are silent losses of exactly the thing
    the caller asked for.
    """


def config(slug: str) -> dict[str, Any] | None:
    """The mdim config for a slug, or None if the slug is not a published mdim.

    Raises:
        MdimLookupError: the registry could not be read at all.
    """
    try:
        from etl.db import read_sql

        df = read_sql(
            "SELECT config FROM multi_dim_data_pages WHERE slug = %(slug)s AND published = 1 LIMIT 1",
            params={"slug": slug},
        )
    except Exception as e:
        raise MdimLookupError(f"could not read the multi-dim registry: {e}") from e
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


def all_published_views(include_side_by_side: bool = False) -> dict[str, list[str]]:
    """``{slug: [params, …]}`` for every published mdim, from one query.

    Used to build a review pool in which an mdim's views compete with ordinary charts. Note the
    scale: a handful of mdims declare over a hundred views each, so this is a few thousand rows.
    """
    try:
        from etl.db import read_sql

        df = read_sql("SELECT slug, config FROM multi_dim_data_pages WHERE published = 1 AND slug IS NOT NULL")
    except Exception as e:
        raise MdimLookupError(f"could not read the multi-dim registry: {e}") from e

    out: dict[str, list[str]] = {}
    for slug, raw in zip(df.slug, df.config):
        cfg: dict[str, Any] = json.loads(raw) if isinstance(raw, str) else (raw or {})
        views = [v["dimensions"] for v in cfg.get("views", []) if v.get("dimensions")]
        if not include_side_by_side:
            views = [v for v in views if not any(str(x).endswith(SIDE_BY_SIDE) for x in v.values())] or views
        if views:
            # Nothing in the schema promises dimension slugs are URL-safe, and an unescaped one
            # would come back as a bundle failure rather than a wrong answer. Latent today:
            # audited 2026-09-02, none of the 40 published mdims has a slug needing escaping.
            out[str(slug)] = [urlencode(combo) for combo in views]
    return out


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
        picked.append((", ".join(f"{k}={v}" for k, v in combo.items()), urlencode(combo)))
    return picked

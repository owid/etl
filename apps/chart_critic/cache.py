"""On-disk cache for chart bundles and reviews, under ``.cache/chart_critic/``.

Two separate things are cached, for different reasons:

- **Bundles** (the render, metadata and numeric summary) are cached with a TTL, because
  downloading them is what makes a sweep slow — a chart's full CSV can be megabytes — and the
  underlying data only changes when a step re-runs.
- **Reviews** are cached against a key that includes a fingerprint of the reviewing algorithm
  *and* a hash of the bundle content. So changing the prompt, the output schema or the bundle
  format invalidates every review automatically, and a chart whose data has changed since the
  cached review is re-reviewed on its own. There is nothing to remember to bump.

That last property is the point. A review tool whose cache outlives a prompt change will
happily report yesterday's answers about today's algorithm, and the failure is silent —
a stale clean result looks exactly like a fresh clean result.

**Caching interacts badly with a flaky model, and the interaction is worth understanding.** The
model raises a genuine finding on some passes and not others: for ``share-elec-by-source``, five
cached passes over the same bundle read CLEAN, FLAGGED, CLEAN, FLAGGED, FLAGGED. Reviews are
cached per pass index, so a single-pass run replays pass 0 — and where pass 0 was a miss, that
chart reports clean on every subsequent run, permanently and invisibly. Two things address it:
``--repeat`` defaults to 3 rather than 1, and every run prints how many passes were served from
cache instead of reviewed. Neither removes the underlying flakiness; they stop it from being
silent.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import time
from pathlib import Path
from typing import Any

from etl.paths import CACHE_DIR

CRITIC_CACHE_DIR = CACHE_DIR / "chart_critic"
DEFAULT_TTL_HOURS = 24


def _source_hash(*module_names: str) -> str:
    """Hash the source of the modules that decide what gets cached."""
    h = hashlib.sha256()
    for name in module_names:
        path = Path(__file__).with_name(name)
        h.update(path.read_bytes() if path.exists() else name.encode())
    return h.hexdigest()


def bundle_fingerprint() -> str:
    """A short hash of the code that builds a bundle.

    The bundle cache needs this as much as the review cache does, and for a reason learned the
    hard way: filtering grapher's ``yAxis: {"max": 0}`` sentinel out of the config summary changed
    what the model is shown, but cached bundles were keyed on the slug alone — so a sweep kept
    replaying the old summary and kept reproducing the false positive the fix had just removed.

    Hashing the source rather than a version constant means nobody has to remember to bump it.
    Editing a comment invalidates bundles too, which is a fine price: re-fetching is cheap and a
    stale bundle is silent.
    """
    return _source_hash("bundle.py", "chart_config.py")[:12]


def algo_fingerprint() -> str:
    """A short hash of everything that determines what a review says."""
    from apps.chart_critic import critic

    material = json.dumps(
        {
            "instructions": critic.INSTRUCTIONS,
            "schema": critic.Review.model_json_schema(),
            "bundle": bundle_fingerprint(),
            "critic_source": _source_hash("critic.py"),
        },
        sort_keys=True,
    )
    return hashlib.sha256(material.encode()).hexdigest()[:12]


def _slug_dir(slug: str) -> Path:
    # Slugs are URL path segments, but be defensive about anything odd.
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in slug)[:120]
    return CRITIC_CACHE_DIR / bundle_fingerprint() / safe


def bundle_paths(slug: str) -> tuple[Path, Path]:
    d = _slug_dir(slug)
    return d / "bundle.json", d / "chart.png"


def read_bundle(slug: str, ttl_hours: float = DEFAULT_TTL_HOURS) -> dict[str, Any] | None:
    """The cached bundle for a slug, or None if absent or older than the TTL."""
    meta_path, png_path = bundle_paths(slug)
    if not meta_path.exists():
        return None
    if ttl_hours >= 0 and (time.time() - meta_path.stat().st_mtime) > ttl_hours * 3600:
        return None
    try:
        payload = json.loads(meta_path.read_text())
    except json.JSONDecodeError:
        return None
    payload["png"] = png_path.read_bytes() if png_path.exists() else None
    return payload


def write_bundle(
    slug: str,
    summary: str,
    png: bytes | None,
    notes: list[str],
    data_available: bool,
    extremes_params: str = "",
) -> None:
    meta_path, png_path = bundle_paths(slug)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(
        json.dumps(
            {
                "summary": summary,
                "notes": notes,
                "data_available": data_available,
                "extremes_params": extremes_params,
            },
            indent=1,
        )
    )
    if png is not None:
        png_path.write_bytes(png)


def content_hash(summary: str, png: bytes | None) -> str:
    """Hash of what the model is actually shown, so changed data invalidates its review."""
    h = hashlib.sha256(summary.encode())
    if png is not None:
        h.update(png)
    return h.hexdigest()[:12]


def _review_path(slug: str, model: str, bundle_hash: str, pass_index: int) -> Path:
    model_safe = model.replace(":", "_").replace("/", "_")
    return _slug_dir(slug) / f"review_{algo_fingerprint()}_{model_safe}_{bundle_hash}_{pass_index}.json"


def read_review(slug: str, model: str, bundle_hash: str, pass_index: int) -> list[dict[str, Any]] | None:
    path = _review_path(slug, model, bundle_hash, pass_index)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())["issues"]
    except (json.JSONDecodeError, KeyError):
        return None


def write_review(slug: str, model: str, bundle_hash: str, pass_index: int, issues: list[dict[str, Any]]) -> None:
    path = _review_path(slug, model, bundle_hash, pass_index)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"issues": issues}, indent=1))


def clear() -> tuple[int, float]:
    """Delete the whole cache. Returns (files removed, megabytes freed)."""
    if not CRITIC_CACHE_DIR.exists():
        return 0, 0.0
    files = [p for p in CRITIC_CACHE_DIR.rglob("*") if p.is_file()]
    size_mb = sum(p.stat().st_size for p in files) / 1e6
    shutil.rmtree(CRITIC_CACHE_DIR)
    return len(files), size_mb


def stats() -> tuple[int, int, float]:
    """(charts cached, review files cached, megabytes on disk)."""
    if not CRITIC_CACHE_DIR.exists():
        return 0, 0, 0.0
    charts = [d for d in CRITIC_CACHE_DIR.iterdir() if d.is_dir()]
    files = [p for p in CRITIC_CACHE_DIR.rglob("*") if p.is_file()]
    reviews = [p for p in files if p.name.startswith("review_")]
    return len(charts), len(reviews), sum(p.stat().st_size for p in files) / 1e6

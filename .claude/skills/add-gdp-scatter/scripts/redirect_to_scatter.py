"""Retire old "X vs. GDP per capita" scatter charts by pointing them at the scatter tab of
the new target charts (part 2 of the add-gdp-scatter workflow).

Reads a JSON list of `{grapher_url, target_chart_url}` from stdin (public
ourworldindata.org/grapher/<slug> URLs). Report-first: without `--apply` it audits what
references each OLD chart and prints the plan, mutating nothing. With `--apply` it registers
the old slug as a chart redirect on the TARGET chart carrying `?tab=scatter&time=latest&country=`,
re-points the old chart's own aliases at the target, and unpublishes the old chart.

Mechanism notes:
- Uses `chart_slug_redirects` — the chart editor's "Alternative URLs for this chart". Since
  grapher #6674 those rows carry a `target_query_param`, which is what makes the scatter tab
  reachable; before that this script had to use the site `redirects` table instead.
- The redirect is consulted only when /grapher/<old-slug> returns a 404, so the unpublish is
  what makes it fire — and, because the create call triggers no static build, also what bakes
  it. Hence the order: create, re-point aliases, unpublish last. One exception, and it is why
  every bail-out rolls the row back: a row touched in the last week is ALSO written into
  `_redirects` as an unconditional 302 to defeat the CDN cache, so a fresh row on a slug whose
  chart is still published does fire.
- On production the redirect MERGES an incoming query over `target_query_param` key by key,
  the incoming side winning per key (distinguishing transcript in
  `build_reference_handoff.params_cell`), so a reference's params cost the reader exactly the
  stored keys they collide with. Staging's serving layer and a fresh row's first-week static
  302 instead answer with the stored query and drop the visitor's params (both verified live
  2026-08-14) — the audits below grade by the production 301 behavior, which takes over when
  the 302 expires.
- `chart_slug_redirects` is per-environment and does NOT sync to production, so prod is a
  separate `--apply --allow-production` run once the scatter views ship there.
"""

import argparse
import importlib.util
import json
import re
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlsplit

from apps.chart_sync.admin_api import AdminAPI
from etl.config import OWID_ENV
from etl.db import get_engine
from etl.http import session as http_session


def _load_sibling(name: str):
    """Import a script from this skill's own scripts directory."""
    return _load_module(name, Path(__file__).resolve().parent / f"{name}.py")


def _load_shared(name: str):
    """Import a find-chart-references module, so the surface definitions stay shared."""
    return _load_module(name, Path(__file__).resolve().parents[2] / "find-chart-references" / "scripts" / f"{name}.py")


def _load_module(name: str, path: Path):
    if not path.exists():
        raise SystemExit(f"Cannot import {name}: {path} does not exist")
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


# The log-source question, and the reversed-source exclusion it depends on, are owned by the
# applier so the three consumers cannot disagree.
applier = _load_sibling("apply_scatter_defaults")
# Featured metrics are matched by the sweep's own reader rather than a local query: the only
# handle a `featured_metrics` row carries is a URL, and `LIKE '%/grapher/<slug>%'` cannot tell
# `/grapher/foo` from `/grapher/foo-bar` (see `featured_metric_rows`).
fr = _load_shared("find_references")

SLUG_RE = re.compile(r"/grapher/([^/?#]+)")
TAILSCALE_SUFFIX_RE = re.compile(r"\.tail[0-9a-z]+\.ts\.net")

# Query string stored on the redirect. Every part of it is load-bearing, because a tab
# supplied in the URL does NOT get the adjustments a tab CLICK does: `adjustStateForTab`
# (which collapses the scatter's time handles and clears its entity selection) is reached
# only from `onTabChange` <- the ContentSwitchers control, while a URL tab goes through
# `populateFromQueryParams` -> `setTab`, which just assigns the tab. So each adjustment has
# to be supplied explicitly here or the reader arriving by an old slug gets a different
# chart from the reader who clicks the tab:
#   tab=scatter  the view itself
#   time=latest  what ensureTimeHandlesAreSensibleForTab would have collapsed to
#   country=     what ensureEntitySelectionIsSensibleForTab would have cleared;
#                `parseCountryParam` returns valid([]) for a present-but-empty value, and
#                `setSelectedEntities([])` clears, so the scatter shows every entity
#                unhighlighted instead of emphasising the target's line/bar selection
TARGET_QUERY = "tab=scatter&time=latest&country="

# Appended for a source whose log y axis describes the non-GDP indicator. The applier leaves
# the target's `yAxis` linear on purpose (it is global, so log would flip the line/bar views),
# so without this a reader arriving by the retired slug gets a linear scatter where the old
# chart was logarithmic — the shape the author chose log to show. `target_query_param` is
# per-row, so unlike the rest of TARGET_QUERY this part can vary by row.
Y_SCALE_LOG = "yScale=log"

# Reference categories a redirect alone does NOT fix — these BLOCK the row.
# narrativeCharts is deliberately not here: one parented to a chart owns a materialized full
# config and keeps rendering after the parent is unpublished, and its only parent-slug link is
# the "Explore the data" href, which the redirect covers. They are reported, never gated on.
MANUAL_REF_KEYS = ["explorers", "dataInsights", "staticViz"]
ALL_REF_KEYS = ["postsWordpress", "postsGdocs", "narrativeCharts", *MANUAL_REF_KEYS]
REF_LABEL = {
    "postsWordpress": "wp",
    "postsGdocs": "gdoc",
    "explorers": "expl",
    "narrativeCharts": "narr",
    "dataInsights": "ins",
    "staticViz": "sviz",
}

# Statuses that still need the alias re-point and the unpublish (EXISTS = redirect already there).
ACTIONABLE = ("CREATE", "UPDATE", "EXISTS")


def short_admin_host() -> str:
    return TAILSCALE_SUFFIX_RE.sub("", OWID_ENV.admin_api).rstrip("/").removesuffix("/api")


def slug_from_url(url: str) -> str:
    m = SLUG_RE.search(url)
    if not m:
        raise ValueError(f"Could not extract /grapher/<slug> from {url!r}")
    return m.group(1)


def chart_ids_for_slugs(slugs: tuple[str, ...]) -> dict[str, int]:
    """Live chart slug -> chart id."""
    if not slugs:
        return {}
    df = OWID_ENV.read_sql(
        "SELECT cc.slug, c.id FROM charts c JOIN chart_configs cc ON c.configId = cc.id WHERE cc.slug IN %(s)s",
        params={"s": slugs},
    )
    return {r["slug"]: int(r["id"]) for r in df.to_dict("records")}


def chart_redirects_for_slugs(slugs: tuple[str, ...]) -> dict[str, dict]:
    """Existing `chart_slug_redirects` rows, keyed by the old slug they serve."""
    if not slugs:
        return {}
    df = OWID_ENV.read_sql(
        "SELECT id, slug, chart_id, target_query_param FROM chart_slug_redirects WHERE slug IN %(s)s",
        params={"s": slugs},
    )
    return {
        r["slug"]: {"id": int(r["id"]), "chart_id": int(r["chart_id"]), "param": r["target_query_param"] or ""}
        for r in df.to_dict("records")
    }


def shadowing_sources(slugs: tuple[str, ...]) -> tuple[dict[str, str], set[str]]:
    """Slugs already claimed by a redirect that would win over a chart redirect.

    A site redirect bakes into `_redirects` as a static 301 matched before the grapher route
    runs. A multi-dim redirect is merged into `_grapherRedirects.json` *after* the chart ones
    (`getGrapherToChartAndMultiDimRedirects` spreads the mdim map second), so it overwrites a
    chart redirect on the same slug. Either way ours would be dead weight.
    """
    if not slugs:
        return {}, set()
    paths = tuple(f"/grapher/{s}" for s in slugs)
    site = OWID_ENV.read_sql("SELECT source, target FROM redirects WHERE source IN %(s)s", params={"s": paths})
    mdim = OWID_ENV.read_sql("SELECT source FROM multi_dim_redirects WHERE source IN %(s)s", params={"s": paths})
    return (
        {r["source"].removeprefix("/grapher/"): r["target"] for r in site.to_dict("records")},
        {str(s).removeprefix("/grapher/") for s in mdim["source"]},
    )


def gdoc_references(slugs: tuple[str, ...]) -> list[dict]:
    """Article links and embeds of these slugs (`posts_gdocs_links`).

    `componentType` is the discriminator: a `span-*` value is a hyperlink in prose, anything
    else is a block-level component that renders the chart itself. The distinction matters
    because the two behave differently after the redirect — see `param_notes`.
    """
    if not slugs:
        return []
    df = OWID_ENV.read_sql(
        "SELECT pgl.target, pg.slug AS post_slug, pg.published, pgl.componentType, pgl.queryString "
        "FROM posts_gdocs_links pgl JOIN posts_gdocs pg ON pg.id = pgl.sourceId "
        "WHERE pgl.target IN %(s)s AND pgl.linkType IN ('grapher', 'guided-chart') ORDER BY pg.slug",
        params={"s": slugs},
    )
    rows = []
    for r in df.to_dict("records"):
        component = str(r["componentType"] or "")
        rows.append(
            {
                "slug": r["target"],
                "post": r["post_slug"],
                "published": bool(r["published"]),
                "kind": "link" if component.startswith("span-") else "embed",
                "component": component or "unknown",
                "query": str(r["queryString"] or ""),
            }
        )
    return rows


def query_keys(query: str) -> set[str]:
    """The param keys in a query string, blank values included — `country=` is a key."""
    return {key for key, _ in parse_qsl(query.lstrip("?"), keep_blank_values=True)}


def narrative_children(query_by_parent: dict[int, str]) -> list[dict]:
    """Narrative charts parented to these charts, with the params they open their parent at.

    `queryParamsForParentChart` is a JSON object (not a query string) that the narrative
    chart's canonical "Explore the data" href appends to the parent slug
    (`GrapherState.canonicalUrlIfIsNarrativeChart`). On production the redirect merges those
    params over the row's stored query KEY BY KEY, the incoming side winning per key (see
    `build_reference_handoff.params_cell` for the distinguishing transcript) — so the href
    costs the reader exactly the stored keys the narrative's params collide with, and keys
    the params don't mention (e.g. a stored `yScale=log`) survive the hop.

    The intersection with the parent row's own stored query is therefore the whole note: it is
    read per-row rather than from a fixed list because only a log source stores a `yScale`.
    Same grading as `param_notes` applies to article links.
    """
    if not query_by_parent:
        return []
    df = OWID_ENV.read_sql(
        "SELECT id, name, parentChartId AS parent_id, queryParamsForParentChart AS params "
        "FROM narrative_charts WHERE parentChartId IN %(ids)s ORDER BY name",
        params={"ids": tuple(sorted(query_by_parent))},
    )
    rows = []
    for r in df.to_dict("records"):
        try:
            params = json.loads(r["params"] or "{}")
        except (TypeError, ValueError):
            params = {}
        parent_id = int(r["parent_id"])
        contradicting = sorted(set(params) & query_keys(query_by_parent[parent_id]))
        if not params:
            note = "no params of its own — the href lands on the redirect's view"
        elif contradicting:
            note = f"its params override {', '.join(contradicting)} in the row's stored query"
        else:
            note = "its params merge over the stored query without touching it — the href keeps the redirect's view"
        rows.append(
            {
                "id": int(r["id"]),
                "name": r["name"],
                "parent_id": parent_id,
                "params": ", ".join(f"{k}={params[k]}" for k in sorted(params)) or "(none)",
                "note": note,
            }
        )
    return rows


def param_notes(ref: dict, stored_query: str) -> str:
    """Which parts of the stored query a given article reference's own params override.

    On production the redirect merges the incoming query over `target_query_param` key by key,
    the incoming side winning per key (see `build_reference_handoff.params_cell` for the
    distinguishing transcript — an earlier version of this audit concluded wholesale
    replacement from a test that could not tell the two models apart). So only collisions with
    `stored_query` — the query THIS row stores, because only a log source stores a `yScale` —
    cost the reader anything; non-colliding params ride along with the stored view intact.
    Same grading as the handoff report's ⚠️.
    """
    notes = []
    if ref["kind"] == "embed":
        # `makeGrapherLinkedChart` builds resolvedUrl without a query string (unlike the
        # multi-dim path), so the target query param never reaches a gdoc-rendered chart.
        notes.append("embed renders the target's default tab")
    keys = query_keys(ref["query"])
    if keys:
        contradicting = sorted(keys & query_keys(stored_query))
        if contradicting:
            notes.append(f"link's own params override {', '.join(contradicting)} in {stored_query}")
    return "; ".join(notes)


def ref_counts(references: dict) -> dict[str, int]:
    return {k: len(references.get(k) or []) for k in ALL_REF_KEYS}


def classify_api_error(exc: Exception) -> tuple[str, str]:
    """Map an admin-API failure to a (status, note) for the report."""
    msg = str(getattr(getattr(exc, "response", None), "text", "") or exc)
    if "duplicate entry" in msg.lower() or "er_dup_entry" in msg.lower():
        # The endpoint does no uniqueness check, so a race with another writer surfaces as a
        # raw MySQL error on `chart_slug_redirects.slug`'s UNIQUE key.
        return "DUPLICATE", f"slug taken since the pre-check: {msg[:90]}"
    return "ERROR", msg[:120]


def row_query(src_id: int | None, log_sources: set[int]) -> str:
    """The query this row stores on its redirect."""
    return f"{TARGET_QUERY}&{Y_SCALE_LOG}" if src_id in log_sources else TARGET_QUERY


def key_chart_slots(src_ids: Iterable[int]) -> dict[int, list[str]]:
    """Topic pages that feature each source as a key chart.

    `get_chart_references` cannot see these — a key chart is a chart-to-tag association
    (`chart_tags.keyChartLevel`), not a row in any reference table — so without this query the
    audit's verdicts look clean while topic pages quietly depend on the chart being unpublished.
    They are the reason a lossy retirement can be unfixable: `GdocPost.loadRelatedCharts` selects
    only `chartId, slug, title, variantName, keyChartLevel` and `RelatedCharts` renders
    `<GrapherWithFallback slug=...>`, so a key-chart slot has nowhere to put a query string —
    neither `tab=scatter` nor `yScale=log` can reach it, at either the source or the target.
    """
    ids = tuple(sorted(set(src_ids)))
    if not ids:
        return {}
    df = OWID_ENV.read_sql(
        "SELECT ct.chartId, t.name FROM chart_tags ct JOIN tags t ON t.id = ct.tagId "
        "WHERE ct.chartId IN %(i)s AND ct.keyChartLevel > 0 ORDER BY t.name",
        params={"i": ids},
    )
    out: dict[int, list[str]] = {}
    for row in df.to_dict("records"):
        out.setdefault(int(row["chartId"]), []).append(str(row["name"]))
    return out


def featured_metric_slots(slugs: Iterable[str]) -> dict[str, list[str]]:
    """Topic-page featured-metric slots holding each source slug, keyed by slug.

    The other param-less surface, and the harsher one: a featured metric names a chart, an MDIM
    view or an explorer view — never a chart's *tab* — so the scatter view cannot be featured at
    all (see "Featured metrics" in SKILL.md). A lossy retirement whose source is featured is
    therefore the case where the loss is not merely unrecoverable by URL but unrecoverable
    outright, which is exactly what the RECONSIDER block exists to surface.
    """
    wanted = {f"/grapher/{s}": (s, None) for s in slugs if s and s != "-"}
    if not wanted:
        return {}
    out: dict[str, list[str]] = {}
    for row in fr.sweep_featured_metrics("chart", wanted):
        out.setdefault(str(row["subject"]), []).append(str(row["where"]))
    return out


def lossy_reasons(loss: dict, tgt_cfg: dict, engine) -> list[dict]:
    """Why retiring this source loses something, in the reader's terms — or [] if it does not.

    Only the losses that survive Part 2 count. A log y axis and the source's exclusions both do:
    the redirect restores the log for a bare slug, but nothing restores it for a reader who
    clicks the scatter tab or for a surface with no query string, and the exclusions are never
    reinstated anywhere. The exclusion grading is the applier's, so this agrees with what Part 1
    printed instead of second-guessing it.

    Each reason carries its `kind`, because the two have different recovery channels and
    `print_reconsider` has to split the blast radius on that: a log axis travels on any URL that
    keeps its params, while an exclusion travels on nothing at all. Reporting them together as
    one "fixable" count overstated what editing an href achieves.
    """
    reasons: list[dict] = []
    if loss.get("log"):
        reasons.append({"kind": "log", "text": "log y axis (the target's scatter tab opens linear)"})
    excluded = loss.get("excluded") or []
    if excluded:
        try:
            tgt_y = applier.find_dim(tgt_cfg, "y") or {}
            y_var_id = tgt_y.get("variableId")
            gdp_var_id = (applier.find_dim(tgt_cfg, "x") or {}).get("variableId")
            if y_var_id is None or gdp_var_id is None:
                raise ValueError("target has no y or no x dimension to grade against")
            tol = int((tgt_y.get("display") or {}).get("tolerance") or 0)
            year = applier.resolve_default_year(tgt_cfg, int(y_var_id), int(gdp_var_id), engine)
            if year is None:
                raise ValueError("no year where both indicators have data")
            graded = applier.classify_exclusions(excluded, int(y_var_id), int(gdp_var_id), year, tol, engine)
            bad = [g for g in graded if g["cls"] in applier.EXCLUSION_WARN_CLASSES]
            if bad:
                reasons.append(
                    {
                        "kind": "exclusions",
                        "text": f"{len(bad)} exclusion(s) that matter: "
                        + ", ".join(f"{g['entity']} {g['cls']}" for g in bad),
                    }
                )
        except Exception as e:
            reasons.append({"kind": "exclusions", "text": f"{len(excluded)} exclusion(s), ungraded ({e!s:.60})"})
    return reasons


def print_reconsider(plan: list[dict]) -> None:
    """The rows where the retirement costs the reader something no redirect gives back.

    Its own block rather than a column, because this is the one verdict here that is warn-only:
    a linear axis or a returning outlier is an editorial loss, not a broken page, so it cannot
    be turned into `BLOCKED` the way a MANUAL reference is without training everyone to pass a
    waiver flag — roughly a fifth of a batch is logarithmic (2026-08-14, batch 1: 3 of 14). A
    column would be missed; a block with the blast radius next to it has to be answered.

    The blast radius is split by whether the fix can travel, and that depends on BOTH the loss
    and the surface. Only a log axis has a recovery channel at all — `yScale=log` on a URL — and
    only a prose link keeps it: an embed drops the query string, a key-chart slot has nowhere to
    put one, and a featured metric cannot even name a chart's tab. An exclusion has no channel
    anywhere, so for that loss every surface is unfixable and none of them counts as "by hand".
    """
    rows = [r for r in plan if r.get("lossy")]
    if not rows:
        return
    print(f"\nRECONSIDER — {len(rows)} retirement(s) lose something the redirect cannot give back")
    print("  Warn-only by design: this is an editorial call, not a broken page. Answer each row with the")
    print("  topic owner before --apply — keeping the standalone chart is a legitimate outcome.")
    for rec in rows:
        refs = rec.get("refs") or {}
        kinds = {r["kind"] for r in rec["lossy"]}
        log_lost, excl_lost = "log" in kinds, "exclusions" in kinds
        links, embeds = rec.get("gdoc_links", 0), rec.get("gdoc_embeds", 0)
        # WordPress rows are prose links on the legacy mirror, so they group with the links.
        carriers = links + refs.get("postsWordpress", 0)
        key_charts = rec.get("key_charts") or []
        featured = rec.get("featured_metrics") or []
        print(f"\n  {rec['src']}  ({rec.get('src_id') or '-'} -> {rec.get('tgt_id') or '-'})")
        for reason in rec["lossy"]:
            print(f"    loses: {reason['text']}")
        if log_lost and carriers:
            print(f"    fixable by hand: {carriers} prose link(s) — add yScale=log to the href")
        if embeds:
            print(
                f"    CANNOT carry the fix: {embeds} gdoc embed(s) (an embed renders the target's DEFAULT tab; "
                f"no query string reaches it)"
            )
        for label, names, why in (
            ("key-chart slot", key_charts, "a chart_tags row has nowhere to put a query string"),
            ("featured metric", featured, "a chart's TAB cannot be featured at all"),
        ):
            if names:
                # The count is per slot while the names are per topic, and one topic can hold
                # several slots — so repeats are collapsed with a multiplicity rather than printed
                # twice, and `len(names)` stays the true number of rows to re-point.
                tally: dict[str, int] = {}
                for n in names:
                    tally[n] = tally.get(n, 0) + 1
                labels = [n if c == 1 else f"{n} x{c}" for n, c in tally.items()]
                shown = ", ".join(labels[:4]) + (f", +{len(labels) - 4} more" if len(labels) > 4 else "")
                print(f"    CANNOT carry the fix: {len(names)} {label}(s) — {shown} ({why})")
        if excl_lost:
            print("    NOTHING carries an exclusion back: the target never re-applies them, so every reference")
            print("    above meets the returning entity however its href is written")
        if not (carriers or embeds or key_charts or featured):
            print("    no references found, so the loss is confined to readers who arrive by the old slug")
        # An embed is as unfixable as a key-chart slot — it was previously counted as "fixable",
        # which is what made an embed-only log row look solved.
        unfixable = embeds + len(key_charts) + len(featured)
        if excl_lost:
            verdict = "an exclusion cannot be restored anywhere — decide whether the scatter reads correctly with the entity back"
        elif unfixable:
            verdict = "the loss lands on pages no query string reaches — decide before applying"
        elif carriers:
            verdict = "every affected surface can carry the params — weigh the edit against keeping the chart"
        else:
            verdict = "the redirect carries yScale=log for old-slug arrivals; only a reader who clicks the target's scatter tab sees it linear"
        print(f"    -> {verdict}")
        # Also on the row itself, so a reader of the PLAN table alone cannot miss it. Appended the
        # same way --allow-manual-refs appends, so a BLOCKED note keeps its own explanation.
        summary = "; ".join(r["text"] for r in rec["lossy"])
        parts = [f"{embeds} gdoc embed(s)"] if embeds else []
        parts += [f"{len(key_charts)} key-chart slot(s)"] if key_charts else []
        parts += [f"{len(featured)} featured metric(s)"] if featured else []
        if parts:
            summary += f"; {' + '.join(parts)} cannot carry the fix"
        if excl_lost:
            summary += "; no surface can carry an exclusion back"
        rec["note"] = (rec["note"] + "  " if rec["note"] else "") + f"[RECONSIDER: {summary}]"


def build_plan(api: AdminAPI, payload: list[dict], skip_aliases: bool) -> list[dict]:
    """Resolve every pair and decide what apply would do — all read-only.

    Runs in both modes on purpose: the audit is only a dry run if it reports the same
    verdicts, so the curator can pull the blocked rows out of the input before applying.
    """
    rows: list[dict[str, Any]] = []
    for row in payload:
        rec: dict[str, Any] = {
            "status": "",
            "note": "",
            "aliases": [],
            "refs": {},
            "manual": False,
            "key_charts": [],
            "featured_metrics": [],
            "loss": {},
            "lossy": [],
            "tgt_cfg": {},
            "gdoc_links": 0,
            "gdoc_embeds": 0,
        }
        try:
            rec |= {"src": slug_from_url(row["grapher_url"]), "tgt": slug_from_url(row["target_chart_url"])}
        except (KeyError, ValueError) as e:
            rec |= {"src": "-", "tgt": "-", "status": "ERROR", "note": str(e)}
        rows.append(rec)

    slugs = tuple(sorted({s for r in rows for s in (r["src"], r["tgt"]) if s != "-"}))
    batch_sources = {r["src"] for r in rows if r["src"] != "-"}
    ids = chart_ids_for_slugs(slugs)
    existing = chart_redirects_for_slugs(slugs)
    site_sources, mdim_sources = shadowing_sources(slugs)
    # What each source carries that the target cannot: a log y axis on the indicator (reversed
    # sources excluded) and its `excludedEntityNames`. One read, one owner — the log half also
    # decides this row's stored query. The exclusions are only counted here; grading them costs
    # indicator data, so `main` does that after `--verify` has had its chance to short-circuit.
    loss = applier.source_lossiness({ids[r["src"]] for r in rows if r["src"] in ids})
    log_sources = {i for i, v in loss.items() if v["log"]}
    key_charts = key_chart_slots(loss.keys())
    for rec in rows:
        src_id = ids.get(rec["src"])
        rec["query"] = row_query(src_id, log_sources)
        rec["loss"] = loss.get(src_id, {})
        rec["key_charts"] = key_charts.get(src_id, [])

    for rec in rows:
        if rec["status"] == "ERROR":
            continue
        src, tgt = rec["src"], rec["tgt"]
        rec["src_id"], rec["tgt_id"] = ids.get(src), ids.get(tgt)

        if src == tgt:
            rec |= {"status": "SKIPPED", "note": "source and target are the same chart"}
            continue
        if rec["src_id"] is None or rec["tgt_id"] is None:
            missing = " and ".join(s for s, i in ((src, rec["src_id"]), (tgt, rec["tgt_id"])) if i is None)
            rec |= {"status": "ERROR", "note": f"slug not resolved: {missing}"}
            continue

        # Target-side guards. These are also the wrong-staging-server detector: a target
        # without a scatter tab usually means this env doesn't have the part-1 changes.
        tgt_cfg = api.get_chart_config(rec["tgt_id"])
        rec["tgt_cfg"] = tgt_cfg
        if "ScatterPlot" not in (tgt_cfg.get("chartTypes") or []):
            rec |= {"status": "SKIPPED", "note": "target has no ScatterPlot tab (wrong env?)"}
            continue
        if not tgt_cfg.get("isPublished"):
            rec |= {"status": "SKIPPED", "note": "target not published"}
            continue

        # The target's own slug must not be redirected away, or we'd build a chain. The batch is
        # checked alongside the database, because a chain formed *within* one input is invisible to
        # the DB queries above and is worse than a chain: retiring the target unpublishes it, and
        # that deletes every redirect pointing at it — including the one this row just created. The
        # source would be left unpublished with no redirect at all, i.e. a hard 404.
        if tgt in batch_sources:
            rec |= {"status": "CHAINED", "note": "another row in this batch retires the target"}
            continue
        if tgt in existing:
            rec |= {"status": "CHAINED", "note": f"target slug is an old slug of chart {existing[tgt]['chart_id']}"}
            continue
        if tgt in site_sources:
            rec |= {"status": "CHAINED", "note": f"site redirect sends the target to {site_sources[tgt]}"}
            continue
        if tgt in mdim_sources:
            rec |= {"status": "CHAINED", "note": "an mdim redirect claims the target slug"}
            continue

        # Source-side claims that would beat our redirect.
        if src in site_sources:
            rec |= {"status": "SITE_EXISTS", "note": f"site redirect already sends it to {site_sources[src]}"}
            continue
        if src in mdim_sources:
            rec |= {"status": "CONFLICT", "note": "an mdim redirect claims this slug and wins over chart redirects"}
            continue

        prior = existing.get(src)
        if prior and prior["chart_id"] != rec["tgt_id"]:
            rec |= {"status": "CONFLICT", "note": f"slug already redirects to chart {prior['chart_id']}"}
            continue
        if prior and prior["param"] == rec["query"]:
            rec |= {"status": "EXISTS", "note": f"-> {tgt}?{rec['query']}"}
        elif prior:
            was = prior["param"] or "(no params)"
            rec |= {"status": "UPDATE", "note": f"param {was} -> {rec['query']}", "prior": prior}
        else:
            rec |= {"status": "CREATE", "note": f"-> {tgt}?{rec['query']}"}

        # Inbound aliases of the SOURCE. Unpublishing it deletes every one of them, so they
        # have to be re-pointed at the target or those URLs become hard 404s. Always read, even
        # under --skip-alias-repoint: the audit should still say what the unpublish will destroy.
        rec["aliases"] = [a for a in api.get_chart_redirects(rec["src_id"]) if a["slug"] != src]
        if skip_aliases and rec["aliases"]:
            # "Leave the aliases alone" and "unpublish the source" are incompatible: the unpublish
            # deletes every redirect pointing at the source, so the flag would silently turn the
            # very slugs it promises to spare into hard 404s. Refuse the row instead — move the
            # aliases by hand, or drop the flag and let the script move them.
            rec |= {
                "status": "BLOCKED",
                "note": f"--skip-alias-repoint, but the unpublish would delete {len(rec['aliases'])} alias(es) of the source",
            }

    # Featured metrics LAST, because they are matched by literal pathname and a slot may well name
    # an inbound alias rather than the current slug — the aliases are only known once the loop
    # above has read them. Unpublishing the source deletes every alias too, so a slot on an old
    # slug empties just the same, and missing it would let RECONSIDER report no parameterless
    # surface for a retirement that silently empties one.
    featured = featured_metric_slots(
        slug for rec in rows for slug in (rec["src"], *(a["slug"] for a in rec["aliases"])) if slug and slug != "-"
    )
    # One entry per SLOT, not per tag. A pathname can hold several slots under one topic tag —
    # different `incomeGroup` rows, say — and each is a separate row someone has to re-point, so
    # de-duplicating by tag name understated the count `print_reconsider` prints. There is nothing
    # to de-duplicate anyway: the sweep visits each `featured_metrics` row once and matches it to a
    # single pathname, and a slug appears once across a row's src plus its aliases.
    for rec in rows:
        slots: list[str] = []
        for slug in (rec["src"], *(a["slug"] for a in rec["aliases"])):
            slots.extend(featured.get(slug, []))
        rec["featured_metrics"] = slots
    return rows


def repoint_alias(api: AdminAPI, alias: dict, src_id: int, tgt_id: int, query: str) -> tuple[str, str]:
    """Move one inbound alias from the source chart to the target, carrying the row's query.

    The delete is mandatory, not a convenience: `chart_slug_redirects.slug` is UNIQUE, so the
    target-side row cannot exist while the source-side one does. An alias's own
    target_query_param is not carried over — it was written against the old chart's config —
    but it is reported so it can be reviewed.
    """
    try:
        api.delete_chart_redirect(alias["id"])
    except Exception as e:
        return "FAILED", f"delete: {classify_api_error(e)[1]}"
    try:
        api.create_chart_redirect(tgt_id, alias["slug"], query)
    except Exception as e:
        status, note = classify_api_error(e)
        try:
            api.create_chart_redirect(src_id, alias["slug"], alias.get("targetQueryParam") or None)
        except Exception as e2:
            return "CRITICAL", f"re-point failed AND restore failed: {note} / restore: {str(e2)[:60]}"
        return status, f"{note} (alias restored on the source)"
    return "REPOINTED", ""


def rollback_primary(api: AdminAPI, rec: dict, created_id: int | None) -> str:
    """Undo this row's own redirect mutation, for any bail-out that leaves the source published.

    A `chart_slug_redirects` row touched in the last week is baked into `_redirects` as an
    unconditional 302, listed ahead of the site redirects (`getRecentChartSlugRedirects`) — that
    copy does not wait for a 404 the way the edge lookup does. So a fresh row on a still-published
    slug sends readers away from a live chart, and every path that decides to keep the source
    published has to take the row back out again.

    For an `UPDATE` that means restoring the row that was replaced, not just deleting the
    replacement: deleting alone would end the run having destroyed a redirect it only meant to
    re-point. The delete triggers the static build, unlike the create.
    """
    if created_id is None:
        return ""
    try:
        api.delete_chart_redirect(created_id)
    except Exception as e:
        return f"; ROLLBACK FAILED, delete redirect {created_id} by hand: {str(e)[:50]}"
    prior = rec.get("prior")
    if not prior:
        return "; redirect rolled back"
    try:
        api.create_chart_redirect(prior["chart_id"], rec["src"], prior["param"] or None)
    except Exception as e:
        return (
            f"; redirect deleted but the one it replaced was NOT restored — re-create {rec['src']}"
            f" -> chart {prior['chart_id']} by hand: {str(e)[:50]}"
        )
    return "; redirect rolled back and the one it replaced restored"


def apply_row(api: AdminAPI, rec: dict, skip_aliases: bool) -> dict:
    """Create/refresh the redirect, re-point the source's aliases, unpublish the source."""
    src_id, tgt_id, src = rec["src_id"], rec["tgt_id"], rec["src"]
    action: dict[str, Any] = {
        "status": rec["status"],
        "note": rec["note"],
        "aliases": [],
        "unpublish": "",
        # Set when this row changed nothing that triggers a static build — see the unpublish branch.
        "needs_bake": False,
    }

    # Read the source config before mutating anything, so an auth/permission failure lands
    # here rather than half-way through.
    try:
        src_cfg = api.get_chart_config(src_id)
    except Exception as e:
        status, note = classify_api_error(e)
        return action | {"status": status, "note": note}

    created_id = None
    if rec["status"] == "UPDATE":
        prior = rec["prior"]
        # No update endpoint, so a wrong target_query_param means delete then re-create. If the
        # re-create fails we put the original row back, so no URL is left unserved.
        try:
            api.delete_chart_redirect(prior["id"])
        except Exception as e:
            status, note = classify_api_error(e)
            return action | {"status": status, "note": note}
        try:
            created_id = api.create_chart_redirect(tgt_id, src, rec["query"])["redirect"]["id"]
            action |= {"status": "UPDATED"}
        except Exception as e:
            status, note = classify_api_error(e)
            try:
                api.create_chart_redirect(prior["chart_id"], src, prior["param"] or None)
                return action | {"status": status, "note": f"{note} (old redirect restored)"}
            except Exception as e2:
                return action | {
                    "status": "CRITICAL",
                    "note": f"replace failed AND restore failed: {note} / restore: {str(e2)[:60]}",
                }
    elif rec["status"] == "CREATE":
        try:
            created_id = api.create_chart_redirect(tgt_id, src, rec["query"])["redirect"]["id"]
            action |= {"status": "CREATED"}
        except Exception as e:
            status, note = classify_api_error(e)
            return action | {"status": status, "note": note}

    # `skip_aliases` only ever skips a no-op here: build_plan BLOCKS a source that still has
    # aliases under that flag, since the unpublish below would delete them.
    if not skip_aliases:
        for alias in rec["aliases"]:
            status, note = repoint_alias(api, alias, src_id, tgt_id, rec["query"])
            action["aliases"].append((alias["slug"], status, note))
        stuck = [slug for slug, status, _ in action["aliases"] if status != "REPOINTED"]
        if stuck:
            # An alias that failed to move is back on the source, and the unpublish would delete
            # it — the exact hard 404 this step exists to prevent. Leave the source published and
            # let a re-run finish the job. The primary redirect cannot be left behind though: the
            # alias delete already triggered a bake, so a row this fresh is a live 302 away from
            # the chart we just decided to keep serving (see rollback_primary).
            note = f"NOT unpublished: fix and re-run, {len(stuck)} alias(es) still on the source ({', '.join(stuck)})"
            moved = [slug for slug, status, _ in action["aliases"] if status == "REPOINTED"]
            if moved:
                note += f"; {len(moved)} alias(es) already moved to the target and left there"
            return action | {"status": "CRITICAL", "unpublish": note + rollback_primary(api, rec, created_id)}

    if not src_cfg.get("isPublished"):
        # Nothing on this path triggers the static build that puts the row into the baked redirect
        # map: the create endpoint doesn't, and there is no unpublish to do it either. (An UPDATE
        # and a re-pointed alias each delete a row, and the delete route does trigger one.) EXISTS
        # counts as well as CREATE: the row may have been written by an earlier run whose deploy
        # failed, or by hand in the chart editor, which bakes nothing either — so "it was already
        # there" is no evidence that it ever went live. main() deploys once for the whole run.
        action["needs_bake"] = rec["status"] in ("CREATE", "EXISTS") and not action["aliases"]
        action["unpublish"] = "already unpublished" + ("; needs a deploy" if action["needs_bake"] else "")
        return action
    try:
        src_cfg["isPublished"] = False
        api.update_chart(src_id, src_cfg)
        action["unpublish"] = "unpublished"
    except Exception as e:
        # The source is still live, so the redirect this row created has to come back out — and an
        # UPDATE has to put back the one it replaced. See rollback_primary.
        note = f"unpublish failed: {str(e)[:70]}" + rollback_primary(api, rec, created_id)
        if action["aliases"]:
            note += "; re-pointed aliases were NOT rolled back — restore them on the source by hand"
        action |= {"status": "CRITICAL", "unpublish": note}
    return action


def verify_redirects(plan: list[dict]) -> int:
    """The skill's closing report: does every redirect actually serve its target on the live site?

    For each target in the plan, every `chart_slug_redirects` row pointing at it is an
    expectation the site must meet: GET /grapher/<slug> answers 30x with a Location whose path
    is the target's slug and whose query is that row's own stored query. That covers the batch's
    old slugs, the aliases the apply re-pointed, and any pre-existing aliases of the target —
    all under the same invariant. Location is compared parsed (path + key/value pairs), never
    as a string, so percent-encoding and key order cannot produce false mismatches.

    The DB rows alone are the batch as the DB currently HOLDS it, not as PLANNED: a planned
    redirect that was deleted, or never created, is simply absent from the query, so a report
    built on the rows alone would certify whatever is left — `0/0 OK` in the worst case, or
    only the target's pre-existing aliases. The plan is the other half of the gate. build_plan
    just re-graded every payload row against the live DB, and a completed batch leaves each one
    at EXISTS (the planned row, storing the planned query); any other status fails the report
    as NOT_APPLIED and names its own reason — CREATE/UPDATE: the row is missing or stores a
    stale query; BLOCKED/CONFLICT/CHAINED/SKIPPED/ERROR: the apply never covered the row.

    Grades: OK (30x to the expected view), NOT_APPLIED (a payload row the DB does not hold as
    planned), NOT_LIVE (200 — the CDN still serves the cached chart page; bake or purge
    pending), NOT_SERVED (404 — the row exists in the DB but nothing serves it yet), MISMATCH
    (30x somewhere else; shown). Exits 1 unless every row is OK, so the report is re-runnable
    until the bake lands and a wrapper can gate on it.
    """
    tgt_by_id = {rec["tgt_id"]: rec["tgt"] for rec in plan if rec.get("tgt_id")}
    if not tgt_by_id:
        print("Nothing to verify: no resolved targets in the plan.")
        return 1
    df = OWID_ENV.read_sql(
        "SELECT slug, chart_id, target_query_param FROM chart_slug_redirects WHERE chart_id IN %(ids)s",
        params={"ids": tuple(sorted(tgt_by_id))},
    )
    in_db = {str(r["slug"]) for r in df.to_dict("records")}
    unmet = [rec for rec in plan if rec.get("status") != "EXISTS" or rec["src"] not in in_db]
    site = OWID_ENV.site.rstrip("/")
    print(f"\nREDIRECT VERIFICATION against {site} ({len(df)} redirect row(s) on {len(tgt_by_id)} target(s))")
    print(f"{'slug':<72} {'grade':<11} note")
    print("-" * 165)
    failures = 0
    for rec in unmet:
        failures += 1
        why = f"plan row at {rec['status'] or 'UNRESOLVED'}" + (f" ({rec['note']})" if rec.get("note") else "")
        print(f"{rec['src']:<72} {'NOT_APPLIED':<11} {why} — the DB does not hold this planned redirect")
    for r in sorted(df.to_dict("records"), key=lambda r: str(r["slug"])):
        tgt_slug = tgt_by_id[int(r["chart_id"])]
        stored = r["target_query_param"] or ""
        expected = f"/grapher/{tgt_slug}" + (f"?{stored}" if stored else "")
        try:
            resp = http_session.get(f"{site}/grapher/{r['slug']}", allow_redirects=False, timeout=30)
        except Exception as e:
            failures += 1
            print(f"{r['slug']:<72} {'ERROR':<11} {e}")
            continue
        if resp.status_code in (301, 302, 307, 308):
            loc = urlsplit(resp.headers.get("Location", ""))
            got_query = sorted(parse_qsl(loc.query, keep_blank_values=True))
            want_query = sorted(parse_qsl(stored, keep_blank_values=True))
            if loc.path == f"/grapher/{tgt_slug}" and got_query == want_query:
                print(f"{r['slug']:<72} {'OK':<11} {resp.status_code} -> {expected}")
            else:
                failures += 1
                print(
                    f"{r['slug']:<72} {'MISMATCH':<11} {resp.status_code} -> {loc.path}?{loc.query}  (expected {expected})"
                )
        elif resp.status_code == 200:
            failures += 1
            print(f"{r['slug']:<72} {'NOT_LIVE':<11} 200 — CDN still serves the cached chart page; bake/purge pending")
        elif resp.status_code == 404:
            failures += 1
            print(f"{r['slug']:<72} {'NOT_SERVED':<11} 404 — row in DB but nothing serves it yet (bake pending)")
        else:
            failures += 1
            print(f"{r['slug']:<72} {'HTTP ' + str(resp.status_code):<11} unexpected answer")
    total = len(df) + len(unmet)
    print(
        f"\n{total - failures}/{total} OK"
        + (
            f" — {failures} not verified; re-run once the bake lands"
            if failures
            else " — every redirect serves its target"
        )
    )
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply", action="store_true", help="create redirects + unpublish sources (otherwise audit only)"
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="closing report: check every redirect row pointing at the batch's targets against the live site; exits non-zero until all serve",
    )
    parser.add_argument(
        "--skip-alias-repoint",
        action="store_true",
        help="leave the sources' own old slugs alone; BLOCKS any source that still has one, since the unpublish would delete it",
    )
    parser.add_argument(
        "--allow-manual-refs",
        action="store_true",
        help="apply rows whose source is referenced by an explorer / data insight / static viz. Only "
        "after those references have been re-pointed: the unpublish breaks them and no redirect covers them.",
    )
    parser.add_argument("--allow-production", action="store_true", help="required to --apply against production")
    args = parser.parse_args()

    payload = json.load(sys.stdin)
    if not isinstance(payload, list):
        print("ERROR: stdin must be a JSON list", file=sys.stderr)
        return 2

    api = AdminAPI(OWID_ENV)
    is_production = OWID_ENV.env_remote == "production"
    print(
        f"Target admin: {short_admin_host()}{'  [PRODUCTION]' if is_production else ''}"
        f"   mode: {'APPLY' if args.apply else 'AUDIT (dry-run)'}"
    )
    if args.apply and is_production and not args.allow_production:
        print("\nREFUSED: --apply against production unpublishes live charts. Re-run with --allow-production.")
        return 2

    if args.verify and args.apply:
        print("\nREFUSED: --verify is the read-only closing report; run it without --apply.")
        return 2

    plan = build_plan(api, payload, skip_aliases=args.skip_alias_repoint)

    if args.verify:
        return verify_redirects(plan)

    # ---- REFERENCES AUDIT (of the OLD source charts) ----
    audited = [r for r in plan if r.get("src_id")]
    for rec in audited:
        refs = api.get_chart_references(rec["src_id"]).get("references", {})
        rec["refs"] = ref_counts(refs)
        rec["manual"] = sum(rec["refs"][k] for k in MANUAL_REF_KEYS) > 0

    engine = get_engine()
    for rec in audited:
        rec["lossy"] = lossy_reasons(rec.get("loss") or {}, rec.get("tgt_cfg") or {}, engine)

    # Article references, split link vs embed, because RECONSIDER's blast radius turns on that:
    # a prose link can be given `yScale=log`, an embed renders the target's default tab whatever
    # its href says. Read once here and reused by the hand-edit table below. Aliases count too —
    # an article linking an older slug lands on the same target.
    rec_by_slug = {slug: rec for rec in audited for slug in (rec["src"], *(a["slug"] for a in rec["aliases"]))}
    query_by_slug = {slug: rec["query"] for slug, rec in rec_by_slug.items()}
    gdoc_rows = gdoc_references(tuple(sorted(query_by_slug)))

    def count_ref(slug: str, kind: str) -> None:
        owner = rec_by_slug.get(slug)
        if owner is not None:
            key = "gdoc_links" if kind == "link" else "gdoc_embeds"
            owner[key] = owner.get(key, 0) + 1

    for row in gdoc_rows:
        count_ref(row["slug"], row["kind"])

    # Raw pasted URLs too. `gdoc_references` reads only `linkType IN ('grapher','guided-chart')`,
    # so an author who pasted a full `/grapher/...` URL (stored as `linkType='url'`) is invisible
    # to it — and since the carrier count is now the link/embed split rather than the aggregate
    # `postsGdocs`, such a reference would otherwise be counted nowhere at all. Delegated to the
    # sweep's own reader, which already unwraps Google redirect wrappers and skips archive hosts.
    # A failure here must not authorise the unpublish. `gdoc_references` above is unguarded and so
    # already fails closed; guarding this one and continuing would have made the pair inconsistent
    # — a reference sweep that could not run leaves the blast radius unknown, and `--apply` is
    # exactly the step that acts on it. So: read-only runs report what they did gather and say the
    # counts are short, while `--apply` refuses outright rather than unpublishing against a partial
    # audit. There is deliberately no waiver flag: retry it, do not wave it through.
    try:
        url_rows = fr.sweep_gdoc_url_links({slug: {"id": rec.get("src_id")} for slug, rec in rec_by_slug.items()})
        for row in url_rows:
            count_ref(str(row["subject"]), "link" if row["kind"] == fr.LINK else "embed")
    except Exception as e:
        if args.apply:
            print(
                f"\nREFUSED: the raw-URL gdoc reference sweep failed ({e!s:.80}). The references audit is "
                f"incomplete, so --apply would unpublish sources whose links and embeds were never counted. "
                f"Re-run without --apply to see the partial audit, then retry once it succeeds."
            )
            return 2
        print(f"\n  (raw-URL gdoc sweep failed, link/embed counts may understate: {e!s:.80})")

    print("\nREFERENCES AUDIT (of the OLD source chart)")
    print(f"{'src_slug':<58} {'src':>5} {'tgt':>5} {'manual':>7} {'lossy':>6} {'keych':>6} {'aliases':>7}  counts")
    print("-" * 155)
    for rec in plan:
        counts = " ".join(f"{REF_LABEL[k]}={v}" for k, v in rec.get("refs", {}).items())
        n_key = len(rec.get("key_charts") or [])
        print(
            f"{rec['src']:<58} {str(rec.get('src_id') or '-'):>5} {str(rec.get('tgt_id') or '-'):>5} "
            f"{'MANUAL' if rec.get('manual') else '':>7} {'LOSSY' if rec.get('lossy') else '':>6} "
            f"{(n_key or ''):>6} {len(rec['aliases']):>7}  {counts}"
        )
    # MANUAL_REF_KEYS says these BLOCK the row, so make that true rather than leaving it to the
    # operator to notice the audit column and re-run: the apply loop gates purely on `status`, so a
    # MANUAL row left at CREATE/UPDATE/EXISTS gets applied and the unpublish breaks the very
    # explorer / data-insight / static-viz references the audit just flagged. Those surfaces embed
    # the old chart's own config, so no redirect can save them.
    for rec in audited:
        if rec.get("manual") and rec["status"] in ACTIONABLE:
            if args.allow_manual_refs:
                rec["note"] = (rec["note"] + "  " if rec["note"] else "") + "[--allow-manual-refs: applying anyway]"
            else:
                blockers = ", ".join(f"{REF_LABEL[k]}={rec['refs'][k]}" for k in MANUAL_REF_KEYS if rec["refs"].get(k))
                rec |= {
                    "status": "BLOCKED",
                    "note": f"direct references a redirect cannot fix ({blockers}) — re-point them first, "
                    f"then pass --allow-manual-refs",
                }

    print("\n  manual = a redirect alone won't fix it: explorers / dataInsights / staticViz reference the old")
    print("           chart directly. Those rows are BLOCKED; re-point them, then --allow-manual-refs.")
    print("  narr   = narrative charts. NOT a blocker (see the table below), but they need replacing.")
    print("  lossy  = the migrated scatter will not look like the chart it replaces (see RECONSIDER below).")
    print("  keych  = key-chart slots on topic pages. Invisible to get_chart_references, and they carry no")
    print("           query string, so they can never show the scatter view — moved or not.")
    print("  aliases = old slugs of the SOURCE chart; the unpublish deletes them, so they get re-pointed too.")

    print_reconsider(plan)

    # ---- NARRATIVE CHARTS PARENTED TO THE OLD CHARTS ----
    by_id = {rec["src_id"]: rec for rec in audited}
    # Each parent's own stored query, so the grading knows whether this row stores a
    # `yScale` for the narrative's own params to contradict on top of erasing.
    narratives = narrative_children({src_id: rec["query"] for src_id, rec in by_id.items()})
    if narratives:
        print("\nNARRATIVE CHARTS ON THE OLD CHARTS (replace, don't re-point)")
        print(f"{'name':<44} {'id':>5}  {'parent params':<30} note")
        print("-" * 150)
        for n in narratives:
            print(f"{n['name']:<44} {n['id']:>5}  {n['params']:<30} {n['note']}")
        print("\n  These keep rendering after the unpublish — they own a materialized full config. Only their")
        print("  'Explore the data' href uses the old slug; the redirect covers the slug, and the notes above")
        print("  say whether the stored view survives the narrative's own params.")
        print("  The parent columns are INSERT-only (owid-grapher#6872, closed as not-planned), so a narrative")
        print("  chart cannot be re-pointed — a replacement, if made, is a new one. Per parent, in this order:")
        for n in narratives:
            # That parent row's own stored query, not the shared constant: a log source's
            # replacement has to carry `yScale=log` too, or the new narrative chart is linear
            # where the one it replaces was logarithmic — the shape the retirement preserves.
            parent = by_id[n["parent_id"]]
            print(f"    {n['name']}: the replacement must reproduce {parent['tgt']}?{parent['query']}, under a NEW")
            print("      kebab-case name; update the article(s) to reference it; then delete the old one last —")
            print(f"      deleting {n['id']} is refused while a published post references the name, and there is")
            print("      no rename.")
        print("\n  Every target here is a plain chart, and a chart page has no 'Create narrative chart' control")
        print("  (it exists only on MDIM views), so creating the replacement is not a UI task. Same options as")
        print("  the handoff: leave the old one in place (it keeps rendering; its 'Explore the data' link")
        print(
            "  loses only the stored keys its params override), ask a developer to create it via the API, or wait for"
        )
        print("  the target to become an MDIM. SKILL.md 'Narrative charts' has the mechanism and citations.")

    # ---- ARTICLE LINKS THAT WON'T LAND ON THE SCATTER ----
    # `gdoc_rows` and `query_by_slug` were read above for RECONSIDER's link/embed split. Aliases
    # are included there: an article may well link the source chart's older slug, and an alias is
    # re-pointed carrying that same query (see repoint_alias), so it has the same keys to collide
    # with. Per-row, because only a log source stores a `yScale` for a reference's own params to
    # contradict.
    refs = []
    for r in gdoc_rows:
        note = param_notes(r, query_by_slug[r["slug"]])
        if note:
            refs.append((r, note))
    if refs:
        print("\nARTICLE REFERENCES NEEDING A HAND EDIT")
        print(f"{'src_slug':<48} {'article':<40} {'kind':<6} {'query':<28} note")
        print("-" * 165)
        for r, note in refs:
            print(f"{r['slug']:<48} {r['post']:<40} {r['kind']:<6} {r['query'][:28]:<28} {note}")
        print("\n  The redirect fires for readers, but these surfaces need editing: a gdoc embed resolves to")
        print("  the target chart and renders its DEFAULT tab (the target query param never reaches it), and")
        print("  a link's own params override the stored keys they collide with (non-colliding params merge in).")

    # ---- PLAN / ACTIONS ----
    print(f"\n{'REDIRECT ACTIONS' if args.apply else 'PLAN (what --apply would do)'}")
    print(f"{'pair':>13}  {'src_slug':<52} {'status':<11} note")
    print("-" * 150)
    critical = False
    needs_bake = False
    for rec in plan:
        pair = f"{rec.get('src_id') or '-'}->{rec.get('tgt_id') or '-'}"
        target = f"-> {rec['tgt']}?{rec['query']}"
        if args.apply and rec["status"] in ACTIONABLE:
            action = apply_row(api, rec, skip_aliases=args.skip_alias_repoint)
            note = f"{action['note']}  [{action['unpublish']}]" if action["unpublish"] else action["note"]
            print(f"{pair:>13}  {rec['src']:<52} {action['status']:<11} {note}")
            for slug, status, alias_note in action["aliases"]:
                print(f"{'alias':>13}  {slug:<52} {status:<11} {alias_note or target}")
            critical = critical or action["status"] == "CRITICAL" or any(a[1] == "CRITICAL" for a in action["aliases"])
            needs_bake = needs_bake or action["needs_bake"]
        else:
            print(f"{pair:>13}  {rec['src']:<52} {rec['status']:<11} {rec['note']}")
            for alias in rec["aliases"]:
                if args.skip_alias_repoint:
                    print(
                        f"{'alias':>13}  {alias['slug']:<52} {'LEFT_ALONE':<11} re-point it by hand, or drop the flag"
                    )
                else:
                    print(f"{'alias':>13}  {alias['slug']:<52} {'WOULD_REPOINT':<11} {target}")

    if args.apply:
        if needs_bake:
            # One deploy covers the whole run: every row above that reported "needs a deploy" only
            # created a redirect (or found one already there) on a source that was already
            # unpublished, and nothing in that path bakes.
            try:
                api.trigger_static_build()
                print("\nDeploy triggered — nothing else in this run would have baked those redirects.")
            except Exception as e:
                critical = True
                print(f"\nDEPLOY FAILED: {str(e)[:100]}")
                print("  Trigger one by hand (admin → Deploy), or those redirects stay out of the baked map.")

        applied = [r for r in plan if r["status"] in ACTIONABLE]
        print("\nVERIFY once the bake finishes (the unpublish triggers it, or the explicit deploy):")
        for rec in applied[:3]:
            url = OWID_ENV.chart_site(rec["src"])
            print(f"  curl -sI {url}   # 301 -> /grapher/{rec['tgt']}?{rec['query']}")
        print("  ...and re-run this script: every row should come back EXISTS.")
        if is_production:
            print("\n  This was production. Nothing else to do — chart_slug_redirects does not sync anywhere.")
        else:
            print("\n  Staging only. Re-run with --apply --allow-production against admin.owid.io once the")
            print("  scatter views are live on production; chart_slug_redirects does not sync from staging.")

    return 1 if critical else 0


if __name__ == "__main__":
    raise SystemExit(main())

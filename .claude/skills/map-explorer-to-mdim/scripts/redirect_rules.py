"""Pure functions mirroring how grapher stores, matches and applies explorer redirects.

Every function here is a port of a specific grapher symbol, named in its docstring. Nothing
in this module touches the DB or the filesystem, so it is the one piece of this skill that
can be exercised without credentials — and it is where the semantics that are easy to get
subtly wrong live:

- an explorer redirect's source condition is matched against the incoming URL's query
  params, so a condition on an EMPTY value can never match (an absent param is not an empty
  one). `strip_empty` exists for that reason and is mandatory, not cosmetic;
- the target view's params WIN over the incoming ones, and every matched source param the
  view does not constrain is DELETED. This is the opposite of how the chart-side audit
  merges a reference's params, which is why that helper is deliberately not shared.

Ported from owid-grapher (read `origin/master`, not a stale checkout):
  packages/@ourworldindata/utils/src/QueryParamDecisionTree.ts
  db/model/MultiDimRedirects.ts
  functions/_common/redirectTools.ts
"""

import csv
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlencode

# Explorer TSV columns whose header ends in one of these are the dimension (widget) columns.
WIDGET_SUFFIXES = ("Dropdown", "Radio", "Checkbox")


def dim_name(col: str) -> str:
    """Human dimension name from an explorer TSV column header (drops the widget suffix)."""
    for suffix in WIDGET_SUFFIXES:
        if col.endswith(suffix):
            return col[: -len(suffix)].strip()
    return col.strip()


def parse_explorer_views(tsv: str) -> tuple[list[str], list[list[str]]]:
    """(dimension names, one row of display values per view) from an explorer TSV.

    Reads the `graphers` block; a view row's dimension columns are those whose header ends
    in a widget suffix. Row ORDER is significant: it is the only identity an explorer view
    has, and `views_fingerprint` hashes it for exactly that reason.
    """
    lines = tsv.split("\n")
    dim_cols: list[int] = []
    names: list[str] = []
    rows: list[list[str]] = []
    in_block = False
    seen_block = False
    for line in lines:
        if not line.startswith("\t"):
            in_block = line.strip() == "graphers"
            if in_block:
                seen_block = True
                dim_cols, names = [], []
            continue
        if not in_block:
            continue
        cells = line[1:].split("\t")
        if not names and not dim_cols:
            for i, col in enumerate(cells):
                # Strip BEFORE both the suffix test and dim_name: a CRLF file leaves "\r" on the
                # last header, which the test tolerates but dim_name does not — so the suffix
                # survived and the dimension was named "Foo Dropdown" instead of "Foo". That
                # name becomes the redirect's condition key, so every URL for those views would
                # miss its rule and fall through to the catch-all, silently.
                col = col.strip()
                if col.endswith(WIDGET_SUFFIXES):
                    dim_cols.append(i)
                    names.append(dim_name(col))
            continue
        rows.append([(cells[i].strip() if i < len(cells) else "") for i in dim_cols])
    # Fail loudly rather than returning an empty grid. An empty grid extracts cleanly,
    # fingerprints cleanly, and builds a catch-all-only payload that silently discards every
    # per-view redirect — so it has to be an error, not a quiet zero.
    if not seen_block:
        raise SystemExit("No 'graphers' block found in the explorer TSV — cannot read its views.")
    if not names:
        raise SystemExit(
            "The explorer's `graphers` block has no dimension columns (no header ending in "
            f"{'/'.join(WIDGET_SUFFIXES)}) — cannot map views without dimensions."
        )
    if not rows:
        raise SystemExit("The explorer's `graphers` block declares dimensions but contains no view rows.")
    return names, rows


def views_fingerprint(dim_names: list[str], rows: list[list[str]]) -> str:
    """Stable digest of an explorer's view grid, as the redirects see it.

    Order-sensitive on purpose: a re-saved TSV that renumbers rows, and a relabelled choice,
    both invalidate the positional view ids that `mapping_proposal.csv`, the review HTML and
    `sourceViewId` all key on — and there is no other fingerprint to catch that (an explorer
    view has no config id or md5 of its own). Empty values are stripped first so the hash
    covers exactly what the redirect conditions key on: a change to a column the redirects
    ignore raises no false alarm.
    """
    payload = "\n".join(
        "\t".join(f"{name}={value}" for name, value in zip(dim_names, row) if value != "") for row in rows
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def strip_empty(dims: dict) -> dict:
    """Drop empty-valued keys from a source condition.

    MANDATORY before posting a payload. The request-time matcher compares a condition value
    against the incoming URL's param, and an absent param is not an empty string — so a
    condition of `{"Period": ""}` can never match, and every view carrying one silently
    falls through to the catch-all instead of its intended target. Keeping them is the one
    mistake that produces a payload the endpoint accepts and then serves wrongly.
    """
    return {k: v for k, v in dims.items() if v != "" and v is not None}


@dataclass(frozen=True)
class SourceRule:
    """One `multi_dim_redirects` row, as the request-time matcher sees it."""

    condition: dict[str, str]  # == the row's sourceQueryParams (already empty-stripped)
    insert_index: int  # 0 = catch-all, then payload order; ties in specificity break on it
    source_view_id: int | None  # None for the catch-all
    mdim: str
    mdim_slug: str
    catalog_path: str
    view_id: str  # "A2"; "" for the catch-all's default view
    target_dims: dict[str, str] = field(default_factory=dict)
    # The rendering this target was reviewed against. Empty for runs extracted before it was
    # recorded, and for a catch-all (which points at whatever the MDIM's default view is).
    view_config_md5: str = ""

    @property
    def specificity(self) -> int:
        return len(self.condition)


def build_source_rules(mapping: dict) -> list[SourceRule]:
    """Every rule a payload built from this mapping would create, in insertion order.

    The catch-all goes first because it is inserted first and therefore carries the lowest
    row id; `target: None` entries are skipped because the endpoint reports them `skipped`
    and creates no row.
    """
    rules: list[SourceRule] = []
    catch_all = mapping.get("catchAll") or {}
    if catch_all.get("target"):
        t = catch_all["target"]
        rules.append(
            SourceRule(
                condition=strip_empty((catch_all.get("source") or {}).get("dimensions") or {}),
                insert_index=0,
                source_view_id=None,
                mdim=t.get("mdim", ""),
                mdim_slug=t.get("mdimSlug", ""),
                catalog_path=t.get("catalogPath", ""),
                view_id=t.get("viewId") or "",
                target_dims=dict(t.get("dimensions") or {}),
                view_config_md5=t.get("viewConfigMd5") or "",
            )
        )
    for entry in mapping.get("redirects") or []:
        t = entry.get("target")
        if not t:
            continue
        rules.append(
            SourceRule(
                condition=strip_empty((entry.get("source") or {}).get("dimensions") or {}),
                insert_index=len(rules),
                source_view_id=entry.get("sourceViewId"),
                mdim=t.get("mdim", ""),
                mdim_slug=t.get("mdimSlug", ""),
                catalog_path=t.get("catalogPath", ""),
                view_id=t.get("viewId") or "",
                target_dims=dict(t.get("dimensions") or {}),
                view_config_md5=t.get("viewConfigMd5") or "",
            )
        )
    return rules


def duplicate_conditions(rules: list[SourceRule]) -> list[tuple[SourceRule, SourceRule]]:
    """Pairs of rules that collapse to the same condition once empties are stripped.

    The endpoint rejects the second of each pair (its duplicate `(source, sourceQueryParams)`
    check), so the row is unreachable. Distinct from many-views-to-one-MDIM-view, which is
    normal and fine — that is `sharedTargetSourceIds`.
    """
    seen: dict[tuple, SourceRule] = {}
    clashes = []
    for rule in rules:
        key = tuple(sorted(rule.condition.items()))
        if key in seen:
            clashes.append((seen[key], rule))
        else:
            seen[key] = rule
    return clashes


def match_query_params(rules: list[SourceRule], query: dict[str, str]) -> SourceRule | None:
    """The rule grapher would pick for these incoming query params.

    Mirrors `buildQueryParamDecisionTree` + `matchQueryParamDecisionTree`. Those build a
    greedy decision tree, but the result they compute is exactly: **the first rule in
    (specificity DESC, insertion order ASC) order whose every condition key equals the
    incoming value.** Following any path down that tree leaves only the rules consistent
    with the branches taken, and a leaf returns the highest-priority such rule — priority
    being the index after a stable sort by descending specificity, i.e. this ordering.

    An absent incoming param matches no condition (the tree sends it to the `default`
    branch, where only rules that don't constrain that key survive). We never emit `null`
    condition values, so the wildcard case cannot arise here.
    """
    for rule in sorted(rules, key=lambda r: (-r.specificity, r.insert_index)):
        if all(query.get(key) == value for key, value in rule.condition.items()):
            return rule
    return None


def build_target_query_params(rule: SourceRule) -> dict[str, str | None]:
    """Params to apply to the redirect target: mirrors `buildTargetQueryParams`.

    Every param the target view sets is applied, so the redirect always lands on that view.
    Every source param that was matched on but that the view does not constrain maps to
    `None` — a signal to REMOVE it from the outgoing URL, since it is explorer-specific and
    has no business in a grapher URL.
    """
    target: dict[str, str | None] = {key: rule.target_dims[key] for key in sorted(rule.target_dims)}
    for key in sorted(rule.condition):
        if key not in rule.target_dims:
            target[key] = None
    return target


def apply_target_query_params(query: dict[str, str], target: dict[str, str | None]) -> dict[str, str]:
    """Outgoing query params: mirrors `getRedirectForExplorerUrl`.

    Starts from the INCOMING params, then applies the target's: a string sets or overrides,
    `None` deletes. So target params win, and params the redirect says nothing about
    (`country`, `time`, `tab`, `facet`) ride through untouched — which is what a reader
    following an old link wants.
    """
    out = dict(query)
    for key, value in target.items():
        if value is None:
            out.pop(key, None)
        else:
            out[key] = value
    return out


@dataclass
class ResolvedUrl:
    """Where a URL pointing at the explorer ends up once the redirects exist."""

    url: str
    rule: SourceRule | None
    match: str  # view | catch-all (no params) | catch-all (stale params) | …
    stale_params: dict[str, str] = field(default_factory=dict)
    leftover_params: dict[str, str] = field(default_factory=dict)


def resolve_explorer_url(
    rules: list[SourceRule],
    query: dict[str, str],
    host: str,
    choice_values: dict[str, set[str]],
    dim_names: list[str] | None = None,
) -> ResolvedUrl:
    """Resolve one referencing URL's params to the view it will land on, and why.

    `choice_values` maps each dimension name to the values the explorer still offers; a
    param naming a dimension but carrying a value that is no longer offered is `stale` —
    the reference is already semi-broken today (the explorer snaps to a default) and after
    the redirect it lands on the MDIM's default view, so it needs authoring attention
    rather than a mechanical URL swap. Distinguishing that from "the link simply had no
    params" is the difference between a real finding and noise.
    """
    dims = set(dim_names or choice_values.keys())
    stale = {k: v for k, v in query.items() if k in choice_values and v not in choice_values[k]}
    rule = match_query_params(rules, query)
    if rule is None:
        return ResolvedUrl(url="", rule=None, match="unmatched (no catch-all)", stale_params=stale)

    target = build_target_query_params(rule)
    merged = apply_target_query_params(query, target)
    url = f"{host}/grapher/{rule.mdim_slug}"
    if merged:
        url += "?" + urlencode(sorted(merged.items()))

    if rule.source_view_id is not None:
        match = "view"
    elif not any(k in dims for k in query):
        match = "catch-all (no params)"
    elif stale:
        match = "catch-all (stale params)"
    else:
        match = "catch-all (partial params)"
    # Explorer-dimension params that survive into the grapher URL because the matched rule
    # did not condition on them. Harmless noise unless the name collides with an
    # unconstrained MDIM dimension slug, which would silently pick a view.
    leftover = {k: v for k, v in merged.items() if k in dims and k not in rule.target_dims}
    return ResolvedUrl(url=url, rule=rule, match=match, stale_params=stale, leftover_params=leftover)


def choice_values(mapping_dir: Path) -> tuple[list[str], dict[str, set[str]]]:
    """(dimension names, value set per dimension) from a run's `explorer_views.csv`.

    The CSV stores dimensions positionally (`dimension_1…`), so the names come from
    `_sources.json`; this reads the pair together so callers cannot mismatch them.
    """
    import json

    sources = json.loads((mapping_dir / "_sources.json").read_text())
    names = list(sources["explorer"]["dimensions"])
    values: dict[str, set[str]] = {name: set() for name in names}
    with open(mapping_dir / "explorer_views.csv", newline="") as f:
        for row in csv.DictReader(f):
            for i, name in enumerate(names, start=1):
                value = (row.get(f"dimension_{i}") or "").strip()
                if value:
                    values[name].add(value)
    return names, values
